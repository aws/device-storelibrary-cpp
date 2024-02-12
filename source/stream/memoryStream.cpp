#include "stream/memoryStream.hpp"
#include <algorithm>
#include <iostream>

namespace aws {
namespace gg {
static constexpr const char *const RecordNotFoundErrorStr = "Record not found";

std::shared_ptr<StreamInterface> MemoryStream::openOrCreate(StreamOptions &&opts) {
    return std::shared_ptr<StreamInterface>(new MemoryStream(std::move(opts)));
}

expected<uint64_t, StreamError> MemoryStream::append(BorrowedSlice d) {
    auto record_size = d.size();
    auto err = remove_records_if_new_record_beyond_max_size(record_size);
    if (err.code != StreamErrorCode::NoError) {
        return err;
    }

    auto seq = _next_sequence_number++;
    _current_size_bytes += d.size();
    _records.emplace_back(OwnedSlice(d), timestamp(), seq, 0);
    return seq;
}

StreamError MemoryStream::remove_records_if_new_record_beyond_max_size(uint32_t record_size) {
    if (record_size > _opts.maximum_size_bytes) {
        return StreamError{StreamErrorCode::RecordTooLarge, {}};
    }

    // Make room if we need more room
    if (_current_size_bytes + record_size > _opts.maximum_size_bytes) {
        // TODO: Bail out early if we have enough space
        _records.erase(std::remove_if(_records.begin(), _records.end(),
                                      [&](const auto &r) {
                                          if (_current_size_bytes + record_size > _opts.maximum_size_bytes) {
                                              _current_size_bytes -= r.data.size();
                                              return true;
                                          }
                                          return false;
                                      }),
                       _records.end());
        _first_sequence_number = _records.front().sequence_number;
    }

    return StreamError{StreamErrorCode::NoError, {}};
}

expected<uint64_t, StreamError> MemoryStream::append(OwnedSlice &&d) {
    auto record_size = d.size();
    auto err = remove_records_if_new_record_beyond_max_size(record_size);
    if (err.code != StreamErrorCode::NoError) {
        return err;
    }

    uint64_t seq = _next_sequence_number++;
    _current_size_bytes += d.size();
    _records.emplace_back(std::move(d), timestamp(), seq, 0);
    return seq;
}

[[nodiscard]] expected<OwnedRecord, StreamError> MemoryStream::read(uint64_t sequence_number,
                                                                    [[maybe_unused]] const ReadOptions &) const {
    if (sequence_number < _first_sequence_number) {
        return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
    }
    for (auto &r : _records) {
        if (r.sequence_number > sequence_number) {
            break;
        }
        if (r.sequence_number == sequence_number) {
            return OwnedRecord{
                // TODO: This is copying the data because the file-based version needs to return an owned record
                OwnedSlice{BorrowedSlice(r.data.data(), r.data.size())},
                r.timestamp,
                r.sequence_number,
                0,
            };
        }
    }

    return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
}

[[nodiscard]] Iterator MemoryStream::openOrCreateIterator(const std::string &identifier, IteratorOptions) {
    return Iterator{WEAK_FROM_THIS(), identifier,
                    _iterators.count(identifier) ? _iterators[identifier] : _first_sequence_number.load()};
}

void MemoryStream::deleteIterator(const std::string &identifier) { _iterators.erase(identifier); }

void MemoryStream::setCheckpoint(const std::string &identifier, uint64_t sequence_number) {
    _iterators[identifier] = sequence_number;
}

} // namespace gg
} // namespace aws