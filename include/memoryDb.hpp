#pragma once
#include "db.hpp"
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

        DBError remove_records_if_new_record_beyond_max_size(size_t record_size);

      public:
        static std::shared_ptr<StreamInterface> openOrCreate(StreamOptions &&);

        expected<uint64_t, DBError> append(BorrowedSlice) override;
        expected<uint64_t, DBError> append(OwnedSlice &&) override;

        [[nodiscard]] expected<OwnedRecord, DBError> read(uint64_t sequence_number) const override {
            return read(sequence_number, 0);
        };
        [[nodiscard]] expected<OwnedRecord, DBError> read(uint64_t sequence_number,
                                                          uint64_t suggested_start) const override;

        [[nodiscard]] Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) override;
        void deleteIterator(const std::string &identifier) override;

        void setCheckpoint(const std::string &, uint64_t) override;
    };
} // namespace gg
} // namespace aws