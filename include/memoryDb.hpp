#pragma once
#include "db.hpp"
#include <list>

namespace aws::gg {
class MemoryStream : public StreamInterface {
  private:
    StreamOptions _opts;
    std::list<OwnedRecord> _records = {};
    std::unordered_map<char, uint64_t> _iterators = {};

    explicit MemoryStream(StreamOptions &&o) : _opts(std::move(o)) {}

    void remove_records_if_new_record_beyond_max_size(size_t record_size);

  public:
    static std::shared_ptr<StreamInterface> openOrCreate(StreamOptions &&);

    uint64_t append(BorrowedSlice) override;
    uint64_t append(OwnedSlice &&) override;

    [[nodiscard]] const OwnedRecord read(uint64_t sequence_number) const override { return read(sequence_number, 0); };
    [[nodiscard]] const OwnedRecord read(uint64_t sequence_number, uint64_t suggested_start) const override;

    [[nodiscard]] Iterator openOrCreateIterator(char identifier, IteratorOptions) override;
    void deleteIterator(char identifier) override;

    void setCheckpoint(char, uint64_t) override;
};
} // namespace aws::gg