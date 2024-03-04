#include "stream/fileStream.hpp"
#include <algorithm>
#include <string>
#include <utility>

namespace aws {
namespace gg {
expected<std::shared_ptr<FileStream>, StreamError> FileStream::openOrCreate(StreamOptions &&opts) noexcept {
    auto stream = std::shared_ptr<FileStream>(new FileStream(std::move(opts)));
    auto err = stream->loadExistingSegments();
    if (!err.ok()) {
        return err;
    }
    return stream;
}

static constexpr int BASE_10 = 10;

static StreamError kvErrorToStreamError(const kv::KVError &kv_err) noexcept {
    auto e = StreamError{StreamErrorCode::WriteError, kv_err.msg};
    switch (kv_err.code) {
    case kv::KVErrorCodes::NoError:
        e = StreamError{StreamErrorCode::NoError, kv_err.msg};
        break;
    case kv::KVErrorCodes::InvalidArguments:
        e = StreamError{StreamErrorCode::InvalidArguments, kv_err.msg};
        break;
    case kv::KVErrorCodes::ReadError:
        e = StreamError{StreamErrorCode::ReadError, kv_err.msg};
        break;
    case kv::KVErrorCodes::DiskFull:
        e = StreamError{StreamErrorCode::DiskFull, kv_err.msg};
        break;
    default:
        break;
    }
    return e;
}

static StreamError fileErrorToStreamError(const FileError &e) noexcept {
    auto err = StreamError{StreamErrorCode::ReadError, e.msg};
    if (e.code == FileErrorCode::DiskFull) {
        err = StreamError{StreamErrorCode::DiskFull, e.msg};
    }
    return err;
}

StreamError FileStream::loadExistingSegments() noexcept {
    auto kv_err_or = kv::KV::openOrCreate(std::move(_opts.kv_options));
    if (!kv_err_or.ok()) {
        return kvErrorToStreamError(kv_err_or.err());
    }
    _kv_store = std::move(kv_err_or.val());

    auto files_or = _opts.file_implementation->list();
    if (!files_or.ok()) {
        return fileErrorToStreamError(files_or.err());
    }

    auto files = std::move(files_or.val());
    for (const auto &f : files) {
        auto idx = f.rfind(".log");
        if (idx != std::string::npos) {
            char *end_ptr = nullptr;
            errno = 0;
            auto base = strtoull(f.c_str(), &end_ptr, BASE_10);
            // Ignore files whose names are not parsable as u64.
            if ((base == 0U && end_ptr == f.c_str()) || errno != 0) {
                continue;
            }
            FileSegment segment{base, _opts.file_implementation, _opts.logger};
            auto err = segment.open(_opts.full_corruption_check_on_open);
            if (!err.ok()) {
                return err;
            }
            _segments.push_back(std::move(segment));
        }
    }

    std::sort(_segments.begin(), _segments.end());

    // Setup our internal state
    if (!_segments.empty()) {
        _next_sequence_number = _segments.back().getHighestSeqNum() + 1U;
        _first_sequence_number = _segments.front().getBaseSeqNum();
        uint64_t size = 0U;
        for (const auto &s : _segments) {
            size += s.totalSizeBytes();
        }
        _current_size_bytes = size;
    }

    return StreamError{StreamErrorCode::NoError, {}};
}

StreamError FileStream::makeNextSegment() noexcept {
    FileSegment segment{_next_sequence_number, _opts.file_implementation, _opts.logger};

    auto err = segment.open(_opts.full_corruption_check_on_open);
    if (!err.ok()) {
        return err;
    }

    _segments.push_back(std::move(segment));
    return StreamError{StreamErrorCode::NoError, {}};
}

expected<uint64_t, StreamError> FileStream::append(BorrowedSlice d, const AppendOptions &append_opts) noexcept {
    std::lock_guard<std::mutex> lock(_segments_lock);

    auto err = removeSegmentsIfNewRecordBeyondMaxSize(d.size());
    if (!err.ok()) {
        return err;
    }

    // Check if we need a new segment because we don't have any, or the last segment is getting too big.
    if (_segments.empty() || _segments.back().totalSizeBytes() >= _opts.minimum_segment_size_bytes) {
        err = makeNextSegment();
        if (!err.ok()) {
            return err;
        }
    }

    // Now append the record into the last segment
    auto &seg = _segments.back();
    auto seq = _next_sequence_number.fetch_add(1);

    auto e = seg.append(d, aws::gg::timestamp(), seq, append_opts.sync_on_append);
    if (!e.ok()) {
        return fileErrorToStreamError(e.err());
    }
    // Only increment the size if successful. On failure, we expect the segment to not keep any partially written
    // data. There could be partly written data if the application dies before truncating, but we'll find that
    // when we startup again later.
    _current_size_bytes += e.val();

    return seq;
}

StreamError FileStream::removeSegmentsIfNewRecordBeyondMaxSize(uint32_t record_size) noexcept {
    if (record_size > _opts.maximum_size_bytes) {
        return StreamError{StreamErrorCode::RecordTooLarge, {}};
    }

    // Make room if we need more room
    while (_current_size_bytes + record_size > _opts.maximum_size_bytes) {
        auto &to_delete = _segments.front();
        _current_size_bytes -= to_delete.totalSizeBytes();
        to_delete.remove();
        // Remove from in-memory
        std::ignore = _segments.erase(_segments.begin());
        _first_sequence_number = _segments.front().getBaseSeqNum();
    }
    return StreamError{StreamErrorCode::NoError, {}};
}

expected<uint64_t, StreamError> FileStream::append(OwnedSlice &&d, const AppendOptions &append_opts) noexcept {
    const auto x = std::move(d);
    return append(BorrowedSlice(x.data(), x.size()), append_opts);
}

expected<OwnedRecord, StreamError> FileStream::read(uint64_t sequence_number,
                                                    const ReadOptions &provided_options) const noexcept {
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
        const auto have_exact_segment =
            sequence_number >= seg.getBaseSeqNum() && sequence_number <= seg.getHighestSeqNum();

        // sequence_number may refer to a corrupted entry from the previous segment.
        // this can happen when stream re-opened and the segment is truncated due to corruption
        // (highest sequence number is changed).
        //
        // in such cases, we should attempt to return the next available record (if configured)
        if (sequence_number < seg.getBaseSeqNum() && read_options.may_return_later_records) {
            find_exact = false;
        }

        if (have_exact_segment || !find_exact) {
            auto val_or = seg.read(sequence_number, read_options);
            if (val_or.ok()) {
                return val_or;
            }
            if ((val_or.err().code == StreamErrorCode::RecordNotFound ||
                 val_or.err().code == StreamErrorCode::RecordDataCorrupted ||
                 val_or.err().code == StreamErrorCode::HeaderDataCorrupted) &&
                read_options.may_return_later_records) {
                // Fallback to the next available record in the next segment (if any)
                find_exact = false;
                // Since we're skipping to a new segment, we should definitely start from the beginning of it
                read_options.suggested_start = 0;
            }
        }
    }

    return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
}

Iterator FileStream::openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept {
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

StreamError FileStream::deleteIterator(const std::string &identifier) noexcept {
    for (size_t i = 0U; i < _iterators.size(); i++) {
        auto iter = _iterators[i];
        if (iter.getIdentifier() == identifier) {
            auto e = iter.remove();
            auto it = _iterators.begin();
            std::advance(it, i);
            std::ignore = _iterators.erase(it);
            return e;
        }
    }
    return StreamError{StreamErrorCode::IteratorNotFound, {}};
}

StreamError FileStream::setCheckpoint(const std::string &identifier, uint64_t sequence_number) noexcept {
    for (auto &iter : _iterators) {
        if (iter.getIdentifier() == identifier) {
            return iter.setCheckpoint(sequence_number);
        }
    }
    return StreamError{StreamErrorCode::IteratorNotFound, {}};
}

PersistentIterator::PersistentIterator(std::string id, uint64_t start, std::shared_ptr<kv::KV> kv) noexcept
    : _id(std::move(id)), _store(std::move(kv)), _sequence_number(start) {
    auto value_or = _store->get(_id);
    if (value_or.ok()) {
        uint64_t last_value;
        // Use memcpy instead of reinterpret cast to avoid UB.
        std::ignore = memcpy(&last_value, value_or.val().data(), sizeof(uint64_t));
        _sequence_number = std::max(start, last_value);
    }
}

StreamError PersistentIterator::setCheckpoint(uint64_t sequence_number) noexcept {
    _sequence_number =
        sequence_number + 1U; // Set the sequence number to the next in line, so that we
                              // point to where we'd want to resume reading (ie. the first unread record).

    const auto e = _store->put(_id, BorrowedSlice{&_sequence_number, sizeof(uint64_t)});
    return kvErrorToStreamError(e);
}

StreamError PersistentIterator::remove() const noexcept {
    const auto e = _store->remove(_id);
    return e.code == kv::KVErrorCodes::KeyNotFound ? StreamError{StreamErrorCode::NoError, {}}
                                                   : kvErrorToStreamError(e);
}

} // namespace gg
} // namespace aws
