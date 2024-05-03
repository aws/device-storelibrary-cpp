#pragma once
#include <aws/store/common/expected.hpp>
#include <aws/store/common/slices.hpp>
#include <aws/store/stream/stream.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace aws {
namespace store {
namespace stream {
class __attribute__((visibility("default"))) MemoryStream : public StreamInterface {
  private:
    StreamOptions _opts;
    std::vector<OwnedRecord> _records{};
    std::unordered_map<std::string, uint64_t> _iterators{};

    explicit MemoryStream(StreamOptions &&o) noexcept : _opts(std::move(o)) {
    }

    StreamError remove_records_if_new_record_beyond_max_size(const uint32_t record_size) noexcept;

  public:
    static std::shared_ptr<MemoryStream> openOrCreate(StreamOptions &&) noexcept;

    common::Expected<uint64_t, StreamError> append(const common::BorrowedSlice,
                                                   const AppendOptions &) noexcept override;

    common::Expected<uint64_t, StreamError> append(common::OwnedSlice &&, const AppendOptions &) noexcept override;

    common::Expected<OwnedRecord, StreamError> read(const uint64_t sequence_number,
                                                    const ReadOptions &) const noexcept override;

    void removeOlderRecords(uint64_t older_than_timestamp_ms) noexcept override;

    Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept override;
    StreamError deleteIterator(const std::string &identifier) noexcept override;

    StreamError setCheckpoint(const std::string &, const uint64_t) noexcept override;

    ~MemoryStream() override = default;
};
} // namespace stream
} // namespace store
} // namespace aws
