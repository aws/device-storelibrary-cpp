#include "stream/memoryStream.hpp"
#include <algorithm>
#include <iostream>

namespace aws {
namespace gg {
static constexpr const char *const RecordNotFoundErrorStr = "Record not found";

std::shared_ptr<MemoryStream> MemoryStream::openOrCreate(StreamOptions &&opts) {
    return std::shared_ptr<MemoryStream>(new MemoryStream(std::move(opts)));
}

expected<uint64_t, StreamError> MemoryStream::append(BorrowedSlice d, const AppendOptions &) {
    auto record_size = d.size();
    auto ok = remove_records_if_new_record_beyond_max_size(record_size);
    if (!ok) {
        return ok;
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

expected<uint64_t, StreamError> MemoryStream::append(OwnedSlice &&d, const AppendOptions &) {
    auto record_size = d.size();
    auto ok = remove_records_if_new_record_beyond_max_size(record_size);
    if (!ok) {
        return ok;
    }

    uint64_t seq = _next_sequence_number++;
    _current_size_bytes += d.size();
    _records.emplace_back(std::move(d), timestamp(), seq, 0);
    return seq;
}

expected<OwnedRecord, StreamError> MemoryStream::read(uint64_t sequence_number, const ReadOptions &) const {
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

Iterator MemoryStream::openOrCreateIterator(const std::string &identifier, IteratorOptions) {
    return Iterator{WEAK_FROM_THIS(), identifier,
                    _iterators.count(identifier) ? _iterators[identifier] : _first_sequence_number.load()};
}

StreamError MemoryStream::deleteIterator(const std::string &identifier) {
    _iterators.erase(identifier);
    return StreamError{StreamErrorCode::NoError, {}};
}

StreamError MemoryStream::setCheckpoint(const std::string &identifier, uint64_t sequence_number) {
    _iterators[identifier] = sequence_number;
    return StreamError{StreamErrorCode::NoError, {}};
}

} // namespace gg
} // namespace aws