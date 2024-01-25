#include "memoryDb.hpp"
#include <algorithm>
#include <iostream>

namespace aws::gg {
std::shared_ptr<StreamInterface> MemoryStream::openOrCreate(StreamOptions &&opts) {
    return std::shared_ptr<StreamInterface>(new MemoryStream(std::move(opts)));
}

uint64_t MemoryStream::append(BorrowedSlice d) {
    auto record_size = d.size();
    remove_records_if_new_record_beyond_max_size(record_size);

    auto seq = _next_sequence_number++;
    _current_size_bytes += d.size();
    _records.emplace_back(OwnedSlice(d), timestamp(), seq, 0);
    return seq;
}

void MemoryStream::remove_records_if_new_record_beyond_max_size(size_t record_size) {
    if (record_size > _opts.maximum_db_size_bytes) {
        throw std::runtime_error("Record too large");
    }

    // Make room if we need more room
    if (_current_size_bytes + record_size > _opts.maximum_db_size_bytes) {
        // TODO: Bail out early if we have enough space
        _records.remove_if([this, record_size](const OwnedRecord &r) {
            if (_current_size_bytes + record_size > _opts.maximum_db_size_bytes) {
                _current_size_bytes -= r.data.size();
                return true;
            }
            return false;
        });
        _first_sequence_number = _records.front().sequence_number;
    }
}

uint64_t MemoryStream::append(OwnedSlice &&d) {
    auto record_size = d.size();
    remove_records_if_new_record_beyond_max_size(record_size);

    uint64_t seq = _next_sequence_number++;
    _current_size_bytes += d.size();
    _records.emplace_back(std::move(d), timestamp(), seq, 0);
    return seq;
}

static constexpr const char *const RecordNotFoundErrorStr = "Record not found";

[[nodiscard]] OwnedRecord MemoryStream::read(uint64_t sequence_number, uint64_t suggested_start) const {
    if (sequence_number < _first_sequence_number) {
        throw std::runtime_error(RecordNotFoundErrorStr);
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

    throw std::runtime_error(RecordNotFoundErrorStr);
}

[[nodiscard]] Iterator MemoryStream::openOrCreateIterator(char identifier, IteratorOptions) {
    return Iterator{WEAK_FROM_THIS(), identifier,
                    _iterators.count(identifier) ? _iterators[identifier] : _first_sequence_number};
}

void MemoryStream::deleteIterator(char identifier) { _iterators.erase(identifier); }

void MemoryStream::setCheckpoint(char identifier, uint64_t sequence_number) {
    _iterators[identifier] = sequence_number;
}

}; // namespace aws::gg