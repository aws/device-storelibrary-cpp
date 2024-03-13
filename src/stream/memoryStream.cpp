#include <algorithm>
#include <atomic>
#include <aws/store/common/expected.hpp>
#include <aws/store/common/slices.hpp>
#include <aws/store/stream/memoryStream.hpp>
#include <aws/store/stream/stream.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace aws {
namespace store {
namespace stream {
std::shared_ptr<MemoryStream> MemoryStream::openOrCreate(StreamOptions &&opts) noexcept {
    // coverity[autosar_cpp14_a20_8_6_violation] constructor is private, cannot use make_shared
    return std::shared_ptr<MemoryStream>(new MemoryStream(std::move(opts)));
}

common::Expected<uint64_t, StreamError> MemoryStream::append(const common::BorrowedSlice d,
                                                             const AppendOptions &) noexcept {
    const auto record_size = d.size();
    auto err = remove_records_if_new_record_beyond_max_size(record_size);
    if (!err.ok()) {
        return err;
    }

    auto seq = _next_sequence_number.fetch_add(1U);
    _current_size_bytes += d.size();
    _records.emplace_back(common::OwnedSlice(d), timestamp(), seq, 0);
    return seq;
}

StreamError MemoryStream::remove_records_if_new_record_beyond_max_size(const uint32_t record_size) noexcept {
    if (record_size > _opts.maximum_size_bytes) {
        return StreamError{StreamErrorCode::RecordTooLarge, {}};
    }

    // Make room if we need more room
    if (_current_size_bytes + record_size > _opts.maximum_size_bytes) {
        // TODO: Bail out early if we have enough space
        // coverity[autosar_cpp14_a23_0_1_violation] we want the implicit iterator conversion
        std::ignore =
            _records.erase(std::remove_if(_records.begin(), _records.end(),
                                          [&](const auto &r) -> bool {
                                              if (_current_size_bytes + record_size > _opts.maximum_size_bytes) {
                                                  _current_size_bytes -= r.data.size();
                                                  return true;
                                              }
                                              return false;
                                          }),
                           _records.cend());
        _first_sequence_number = _records.front().sequence_number;
    }

    return StreamError{StreamErrorCode::NoError, {}};
}

common::Expected<uint64_t, StreamError> MemoryStream::append(common::OwnedSlice &&d, const AppendOptions &) noexcept {
    auto data = std::move(d);
    const auto record_size = data.size();
    auto err = remove_records_if_new_record_beyond_max_size(record_size);
    if (!err.ok()) {
        return err;
    }

    uint64_t seq = _next_sequence_number.fetch_add(1U);
    _current_size_bytes += data.size();
    _records.emplace_back(std::move(data), timestamp(), seq, 0);
    return seq;
}

common::Expected<OwnedRecord, StreamError> MemoryStream::read(const uint64_t sequence_number,
                                                              const ReadOptions &) const noexcept {
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
                common::OwnedSlice{common::BorrowedSlice(r.data.data(), r.data.size())},
                r.timestamp,
                r.sequence_number,
                0U,
            };
        }
    }

    return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
}

Iterator MemoryStream::openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept {
    return Iterator{WEAK_FROM_THIS(), identifier,
                    _iterators.count(identifier) > 0U ? _iterators[identifier] : _first_sequence_number.load()};
}

StreamError MemoryStream::deleteIterator(const std::string &identifier) noexcept {
    std::ignore = _iterators.erase(identifier);
    return StreamError{StreamErrorCode::NoError, {}};
}

StreamError MemoryStream::setCheckpoint(const std::string &identifier, const uint64_t sequence_number) noexcept {
    _iterators[identifier] = sequence_number;
    return StreamError{StreamErrorCode::NoError, {}};
}

} // namespace stream
} // namespace store
} // namespace aws
