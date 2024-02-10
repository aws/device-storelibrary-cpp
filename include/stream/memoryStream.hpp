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

        explicit MemoryStream(StreamOptions &&o) : _opts(std::move(o)) {}

        StreamError remove_records_if_new_record_beyond_max_size(size_t record_size);

      public:
        static std::shared_ptr<StreamInterface> openOrCreate(StreamOptions &&);

        expected<uint64_t, StreamError> append(BorrowedSlice) override;
        expected<uint64_t, StreamError> append(OwnedSlice &&) override;

        [[nodiscard]] expected<OwnedRecord, StreamError> read(uint64_t sequence_number,
                                                              const ReadOptions &) const override;

        [[nodiscard]] Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) override;
        void deleteIterator(const std::string &identifier) override;

        void setCheckpoint(const std::string &, uint64_t) override;
    };
} // namespace gg
} // namespace aws