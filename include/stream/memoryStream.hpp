#pragma once
#include "stream.hpp"
#include <unordered_map>
#include <vector>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    class MemoryStream final : public StreamInterface {
      private:
        StreamOptions _opts;
        std::vector<OwnedRecord> _records{};
        std::unordered_map<std::string, uint64_t> _iterators{};

        explicit MemoryStream(StreamOptions &&o) noexcept : _opts(std::move(o)) {}

        StreamError remove_records_if_new_record_beyond_max_size(uint32_t record_size) noexcept;

      public:
        static std::shared_ptr<MemoryStream> openOrCreate(StreamOptions &&) noexcept;

        virtual expected<uint64_t, StreamError> append(BorrowedSlice, const AppendOptions &) noexcept override;
        virtual expected<uint64_t, StreamError> append(OwnedSlice &&, const AppendOptions &) noexcept override;

        virtual expected<OwnedRecord, StreamError> read(uint64_t sequence_number,
                                                        const ReadOptions &) const noexcept override;

        virtual Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept override;
        virtual StreamError deleteIterator(const std::string &identifier) noexcept override;

        virtual StreamError setCheckpoint(const std::string &, uint64_t) noexcept override;
    };
} // namespace gg
} // namespace aws