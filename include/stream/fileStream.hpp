#pragma once
#include "kv/kv.hpp"
#include "stream.hpp"
#include <climits>
#include <string>
#include <vector>

namespace aws {
namespace gg {
struct LogEntryHeader;
class FileSegment {
  public:
    FileSegment(uint64_t base, std::shared_ptr<FileSystemInterface>);

    FileSegment(FileSegment &&s) noexcept
        : _f(std::move(s._f)), _file_implementation(std::move(s._file_implementation)), _base_seq_num(s._base_seq_num),
          _highest_seq_num(s._highest_seq_num.load()), _total_bytes(s._total_bytes.load()),
          _segment_id(std::move(s._segment_id)){};

    FileSegment(const FileSegment &) = delete; // non-copyable
    FileSegment &operator=(const FileSegment &) = delete;
    FileSegment &operator=(FileSegment &&s) noexcept {
        _f.swap(s._f);
        _file_implementation.swap(s._file_implementation);
        _base_seq_num = s._base_seq_num;
        _highest_seq_num = s._highest_seq_num.load();
        _total_bytes = s._total_bytes.load();
        _segment_id = s._segment_id;
        return *this;
    };

    ~FileSegment() = default;

    FileError open();

    bool operator<(const FileSegment &other) const { return _base_seq_num < other._base_seq_num; }

    void append(BorrowedSlice d, int64_t timestamp_ms, uint64_t sequence_number);

    expected<OwnedRecord, StreamError> read(uint64_t sequence_number, uint64_t suggested_start) const;

    void remove();

    std::uint64_t getBaseSeqNum() const { return _base_seq_num; }

    std::uint64_t getHighestSeqNum() const { return _highest_seq_num; }

    std::uint64_t totalSizeBytes() const { return _total_bytes; }

  private:
    std::unique_ptr<FileLike> _f;
    std::shared_ptr<FileSystemInterface> _file_implementation{};
    std::uint64_t _base_seq_num{1};
    std::atomic_uint64_t _highest_seq_num{0};
    std::atomic_uint64_t _total_bytes{0};
    std::string _segment_id;

    expected<OwnedRecord, StreamError> getRecord(uint64_t sequence_number, size_t offset, bool suggested_start) const;

    static LogEntryHeader const *convertSliceToHeader(const OwnedSlice &);
};

class PersistentIterator {
  public:
    PersistentIterator(std::string id, uint64_t start, std::shared_ptr<kv::KV>);

    uint64_t getSequenceNumber() const { return _sequence_number; }
    const std::string &getIdentifier() const { return _id; }
    void setCheckpoint(uint64_t);
    void remove();

  private:
    std::string _id;
    std::shared_ptr<kv::KV> _store;
    uint64_t _sequence_number{0};
};

class __attribute__((visibility("default"))) FileStream : public StreamInterface {
  private:
    StreamOptions _opts;
    std::shared_ptr<kv::KV> _kv_store{};
    std::vector<PersistentIterator> _iterators{};
    std::vector<FileSegment> _segments{};

    explicit FileStream(StreamOptions &&o) : _opts(std::move(o)) {
        _iterators.reserve(1);
        _segments.reserve(1 + ((_opts.maximum_size_bytes - 1) / _opts.minimum_segment_size_bytes));
    }

    [[nodiscard]] StreamError removeSegmentsIfNewRecordBeyondMaxSize(size_t record_size);

    [[nodiscard]] FileError makeNextSegment();
    [[nodiscard]] FileError loadExistingSegments();

  public:
    [[nodiscard]] static expected<std::shared_ptr<StreamInterface>, FileError> openOrCreate(StreamOptions &&);

    expected<uint64_t, StreamError> append(BorrowedSlice) override;
    expected<uint64_t, StreamError> append(OwnedSlice &&) override;

    [[nodiscard]] expected<OwnedRecord, StreamError> read(uint64_t sequence_number) const override {
        return read(sequence_number, 0);
    };
    [[nodiscard]] expected<OwnedRecord, StreamError> read(uint64_t sequence_number,
                                                          uint64_t suggested_start) const override;

    [[nodiscard]] Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) override;
    void deleteIterator(const std::string &identifier) override;

    void setCheckpoint(const std::string &, uint64_t) override;
};

} // namespace gg
} // namespace aws