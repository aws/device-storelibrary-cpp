#include <aws/store/common/expected.hpp>
#include <aws/store/common/slices.hpp>
#include <aws/store/stream/stream.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace aws {
namespace store {
namespace stream {
Iterator &Iterator::operator++() noexcept {
    ++sequence_number;
    timestamp = 0;
    return *this;
}

common::Expected<CheckpointableOwnedRecord, StreamError> Iterator::operator*() noexcept {
    if (const auto stream = _stream.lock()) {
        auto record_or = stream->read(sequence_number, ReadOptions{true, true, _offset});
        if (!record_or.ok()) {
            return record_or.err();
        }
        auto x = std::move(record_or.val());
        timestamp = x.timestamp;
        _offset = x.offset + x.data.size();
        sequence_number = x.sequence_number;
        // coverity[autosar_cpp14_a7_1_7_violation] defining lambda inline
        return CheckpointableOwnedRecord{std::move(x), [this]() -> StreamError { return checkpoint(); }};
    }
    return StreamError{StreamErrorCode::StreamClosed, "Unable to read from destroyed stream"};
}

Iterator &&Iterator::begin() noexcept {
    return std::move(*this);
}

// Iterator never ends
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Iterator Iterator::end() noexcept {
    return Iterator{{}, {}, 0};
}

bool Iterator::operator!=(const Iterator &x) const noexcept {
    static_cast<void>(x); // unused parameter required as part of interface
    return true;
}

std::uint64_t StreamInterface::firstSequenceNumber() const noexcept {
    return _first_sequence_number;
}

std::uint64_t StreamInterface::highestSequenceNumber() const noexcept {
    return _next_sequence_number - 1U;
}

std::uint64_t StreamInterface::currentSizeBytes() const noexcept {
    return _current_size_bytes;
}

StreamError Iterator::checkpoint() const noexcept {
    auto e = StreamError{StreamErrorCode::StreamClosed, "Unable to read from destroyed stream"};
    if (const auto stream = _stream.lock()) {
        e = stream->setCheckpoint(_id, sequence_number);
    }
    return e;
}

Iterator::Iterator(std::weak_ptr<StreamInterface> s, std::string id, const uint64_t seq) noexcept
    : _stream(std::move(s)), _id(std::move(id)), sequence_number(seq) {
}

int64_t timestamp() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

OwnedRecord::OwnedRecord(common::OwnedSlice &&idata, const int64_t itimestamp, const uint64_t isequence_number,
                         const uint32_t ioffset) noexcept
    : offset(ioffset), data(std::move(idata)), timestamp(itimestamp), sequence_number(isequence_number) {
}

CheckpointableOwnedRecord::CheckpointableOwnedRecord(OwnedRecord &&o,
                                                     std::function<StreamError()> &&checkpoint) noexcept
    : OwnedRecord(std::move(o)), _checkpoint(std::move(checkpoint)) {
}

void CheckpointableOwnedRecord::checkpoint() const noexcept {
    _checkpoint();
}
} // namespace stream
} // namespace store
} // namespace aws
