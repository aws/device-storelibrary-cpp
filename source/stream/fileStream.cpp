#include "stream/fileStream.hpp"
#include <algorithm>
#include <string>
#include <utility>

namespace aws {
namespace gg {
expected<std::shared_ptr<FileStream>, StreamError> FileStream::openOrCreate(StreamOptions &&opts) {
    auto stream = std::shared_ptr<FileStream>(new FileStream(std::move(opts)));
    auto err = stream->loadExistingSegments();
    if (err.code != StreamErrorCode::NoError) {
        return err;
    }
    return stream;
}

static constexpr int BASE_10 = 10;

StreamError FileStream::loadExistingSegments() {
    auto kv_err_or = kv::KV::openOrCreate(std::move(_opts.kv_options));
    if (!kv_err_or) {
        // TODO: Better error mapping
        switch (kv_err_or.err().code) {
        case kv::KVErrorCodes::InvalidArguments:
            return StreamError{StreamErrorCode::InvalidArguments, kv_err_or.err().msg};
        case kv::KVErrorCodes::ReadError:
            return StreamError{StreamErrorCode::ReadError, kv_err_or.err().msg};
        default:
            return StreamError{StreamErrorCode::WriteError, kv_err_or.err().msg};
        }
    }
    _kv_store = std::move(kv_err_or.val());

    auto files_or = _opts.file_implementation->list();
    if (!files_or) {
        return StreamError{StreamErrorCode::ReadError, files_or.err().msg};
    }

    auto files = std::move(files_or.val());
    for (const auto &f : files) {
        auto idx = f.rfind(".log");
        if (idx != std::string::npos) {
            char *end_ptr = nullptr;
            errno = 0;
            auto base = strtoull(f.c_str(), &end_ptr, BASE_10);
            // Ignore files whose names are not parsable as u64.
            if ((base == 0 && end_ptr == f.c_str()) || errno != 0) {
                continue;
            }
            FileSegment segment{base, _opts.file_implementation, _opts.logger};
            auto err = segment.open(_opts.full_corruption_check_on_open);
            if (err.code != StreamErrorCode::NoError) {
                return err;
            }
            _segments.push_back(std::move(segment));
        }
    }

    std::sort(_segments.begin(), _segments.end());

    // Setup our internal state
    if (!_segments.empty()) {
        _next_sequence_number = _segments.back().getHighestSeqNum() + 1;
        _first_sequence_number = _segments.front().getBaseSeqNum();
        uint64_t size = 0;
        for (const auto &s : _segments) {
            size += s.totalSizeBytes();
        }
        _current_size_bytes = size;
    }

    return StreamError{StreamErrorCode::NoError, {}};
}

StreamError FileStream::makeNextSegment() {
    FileSegment segment{_next_sequence_number, _opts.file_implementation, _opts.logger};

    auto err = segment.open(_opts.full_corruption_check_on_open);
    if (err.code != StreamErrorCode::NoError) {
        return err;
    }

    _segments.push_back(std::move(segment));
    return StreamError{StreamErrorCode::NoError, {}};
}

expected<uint64_t, StreamError> FileStream::append(BorrowedSlice d, const AppendOptions &append_opts) {
    std::lock_guard<std::mutex> lock(_segments_lock);

    auto err = removeSegmentsIfNewRecordBeyondMaxSize(d.size());
    if (err.code != StreamErrorCode::NoError) {
        return err;
    }

    // Check if we need a new segment because we don't have any, or the last segment is getting too big.
    if (_segments.empty() || _segments.back().totalSizeBytes() >= _opts.minimum_segment_size_bytes) {
        err = makeNextSegment();
        if (err.code != StreamErrorCode::NoError) {
            return err;
        }
    }

    // Now append the record into the last segment
    auto &seg = _segments.back();
    auto seq = _next_sequence_number.fetch_add(1);

    auto e = seg.append(d, aws::gg::timestamp(), seq, append_opts.sync_on_append);
    if (!e) {
        return StreamError{StreamErrorCode::WriteError, e.err().msg};
    }
    // Only increment the size if successful. On failure, we expect the segment to not keep any partially written
    // data. There could be partly written data if the application dies before truncating, but we'll find that
    // when we startup again later.
    _current_size_bytes += e.val();

    return seq;
}

StreamError FileStream::removeSegmentsIfNewRecordBeyondMaxSize(uint32_t record_size) {
    if (record_size > _opts.maximum_size_bytes) {
        return StreamError{StreamErrorCode::RecordTooLarge, {}};
    }

    // Make room if we need more room
    while (_current_size_bytes + record_size > _opts.maximum_size_bytes) {
        auto &to_delete = _segments.front();
        _current_size_bytes -= to_delete.totalSizeBytes();
        to_delete.remove();
        // Remove from in-memory
        _segments.erase(_segments.begin());
        _first_sequence_number = _segments.front().getBaseSeqNum();
    }
    return StreamError{StreamErrorCode::NoError, {}};
}

expected<uint64_t, StreamError> FileStream::append(OwnedSlice &&d, const AppendOptions &append_opts) {
    auto x = std::move(d);
    return append(BorrowedSlice(x.data(), x.size()), append_opts);
}

[[nodiscard]] expected<OwnedRecord, StreamError> FileStream::read(uint64_t sequence_number,
                                                                  const ReadOptions &provided_options) const {
    if (sequence_number < _first_sequence_number || sequence_number >= _next_sequence_number) {
        return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
    }

    std::lock_guard<std::mutex> lock(_segments_lock); // Ideally this would be a shared_mutex so we can read in parallel

    auto read_options = provided_options;

    // Initially we will try to find exactly the requested record by sequence number.
    // If we cannot find it and the user has elected to allow later records to be returned,
    // we will try to find the next available record.
    // We will continue to the next available record if any record is not found, data, or header corruption.
    bool find_exact = true;
    for (const auto &seg : _segments) {
        bool have_exact_segment = sequence_number >= seg.getBaseSeqNum() && sequence_number <= seg.getHighestSeqNum();

        if (have_exact_segment || !find_exact) {
            auto val_or = seg.read(sequence_number, read_options);

            if (val_or) {
                return val_or;
            } else if ((val_or.err().code == StreamErrorCode::RecordNotFound ||
                        val_or.err().code == StreamErrorCode::RecordDataCorrupted ||
                        val_or.err().code == StreamErrorCode::HeaderDataCorrupted) &&
                       read_options.may_return_later_records) {
                // Fallback to the next available record in the next segment (if any)
                find_exact = false;
                // Since we're skipping to a new segment, we should definitely start from the beginning of it
                read_options.suggested_start = 0;
                continue;
            }
        }
    }

    return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
}

[[nodiscard]] Iterator FileStream::openOrCreateIterator(const std::string &identifier, IteratorOptions) {
    for (const auto &iter : _iterators) {
        if (iter.getIdentifier() == identifier) {
            return Iterator{WEAK_FROM_THIS(), identifier,
                            std::max(_first_sequence_number.load(), iter.getSequenceNumber())};
        }
    }

    _iterators.emplace_back(identifier, _first_sequence_number, _kv_store);
    return Iterator{WEAK_FROM_THIS(), identifier,
                    std::max(_first_sequence_number.load(), _iterators.back().getSequenceNumber())};
}

void FileStream::deleteIterator(const std::string &identifier) {
    for (size_t i = 0; i < _iterators.size(); i++) {
        auto iter = _iterators[i];
        if (iter.getIdentifier() == identifier) {
            iter.remove();
            auto it = _iterators.begin();
            std::advance(it, i);
            _iterators.erase(it);
            break;
        }
    }
}

void FileStream::setCheckpoint(const std::string &identifier, uint64_t sequence_number) {
    for (auto &iter : _iterators) {
        if (iter.getIdentifier() == identifier) {
            iter.setCheckpoint(sequence_number);
            return;
        }
    }
}

PersistentIterator::PersistentIterator(std::string id, uint64_t start, std::shared_ptr<kv::KV> kv)
    : _id(std::move(id)), _store(std::move(kv)), _sequence_number(start) {
    auto value_or = _store->get(_id);
    if (value_or) {
        uint64_t last_value;
        // Use memcpy instead of reinterpret cast to avoid UB.
        memcpy(&last_value, value_or.val().data(), sizeof(uint64_t));
        _sequence_number = std::max(start, last_value);
    }
}

void PersistentIterator::setCheckpoint(uint64_t sequence_number) {
    _sequence_number = sequence_number + 1; // Set the sequence number to the next in line, so that we
                                            // point to where we'd want to resume reading (ie. the first unread record).

    // TODO: something with the error
    [[maybe_unused]] auto _ = _store->put(_id, BorrowedSlice{&_sequence_number, sizeof(uint64_t)});
}

void PersistentIterator::remove() {
    // TODO: something with the error
    [[maybe_unused]] auto _ = _store->remove(_id);
}

} // namespace gg
}; // namespace aws
