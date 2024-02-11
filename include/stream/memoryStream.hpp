#pragma once
#include "stream.hpp"
#include <list>
#include <unordered_map>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    class MemoryStream : public StreamInterface {
      private:
        StreamOptions _opts;
        std::list<OwnedRecord> _records = {};
        std::unordered_map<std::string, uint64_t> _iterators = {};

        explicit MemoryStream(StreamOptions &&o) noexcept : _opts(std::move(o)) {}

        StreamError remove_records_if_new_record_beyond_max_size(uint32_t record_size) noexcept;

      public:
        static std::shared_ptr<StreamInterface> openOrCreate(StreamOptions &&) noexcept;

        expected<uint64_t, StreamError> append(BorrowedSlice) noexcept override;
        expected<uint64_t, StreamError> append(OwnedSlice &&) noexcept override;

        [[nodiscard]] expected<OwnedRecord, StreamError> read(uint64_t sequence_number,
                                                              const ReadOptions &) const noexcept override;

        [[nodiscard]] Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept override;
        void deleteIterator(const std::string &identifier) noexcept override;

        void setCheckpoint(const std::string &, uint64_t) noexcept override;
    };
} // namespace gg
} // namespace aws