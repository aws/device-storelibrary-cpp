#pragma once
#include "kv/kv.hpp"
#include "stream.hpp"
#include <climits>
#include <mutex>
#include <string>
#include <vector>

namespace aws {
namespace gg {
static constexpr const char *const RecordNotFoundErrorStr = "Record not found";

struct LogEntryHeader;
class FileSegment {
  public:
    FileSegment(uint64_t base, std::shared_ptr<FileSystemInterface>, std::shared_ptr<logging::Logger>) noexcept;

    FileSegment(FileSegment &&s) = default;
    FileSegment &operator=(FileSegment &&s) = default;

    FileSegment(const FileSegment &) = delete;
    FileSegment &operator=(const FileSegment &) = delete;

    ~FileSegment() = default;

    StreamError open(bool full_corruption_check_on_open) noexcept;

    bool operator<(const FileSegment &other) const noexcept { return _base_seq_num < other._base_seq_num; }

    expected<uint64_t, FileError> append(BorrowedSlice d, int64_t timestamp_ms, uint64_t sequence_number,
                                         bool sync) noexcept;

    expected<OwnedRecord, StreamError> read(uint64_t sequence_number, const ReadOptions &) const noexcept;

    void remove() noexcept;

    std::uint64_t getBaseSeqNum() const noexcept { return _base_seq_num; }

    std::uint64_t getHighestSeqNum() const noexcept { return _highest_seq_num; }

    std::uint32_t totalSizeBytes() const noexcept { return _total_bytes; }

  private:
    std::unique_ptr<FileLike> _f;
    std::shared_ptr<FileSystemInterface> _file_implementation{};
    std::shared_ptr<logging::Logger> _logger;
    std::uint64_t _base_seq_num{1U};
    std::uint64_t _highest_seq_num{0U};
    std::uint32_t _total_bytes{0U};
    std::string _segment_id;

    static LogEntryHeader convertSliceToHeader(const OwnedSlice &) noexcept;

    void truncateAndLog(uint32_t truncate, const StreamError &err) const noexcept;
};

class PersistentIterator {
  public:
    PersistentIterator(std::string id, uint64_t start, std::shared_ptr<kv::KV>) noexcept;

    uint64_t getSequenceNumber() const noexcept { return _sequence_number; }
    const std::string &getIdentifier() const noexcept { return _id; }
    StreamError setCheckpoint(uint64_t) noexcept;
    StreamError remove() const noexcept;

  private:
    std::string _id;
    std::shared_ptr<kv::KV> _store;
    uint64_t _sequence_number{0U};
};

class __attribute__((visibility("default"))) FileStream final : public StreamInterface {
  private:
    mutable std::mutex _segments_lock{}; // TODO: would like this to be a shared_mutex, but that is c++17.
    StreamOptions _opts;
    std::shared_ptr<kv::KV> _kv_store{};
    std::vector<PersistentIterator> _iterators{};
    std::vector<FileSegment> _segments{};

    explicit FileStream(StreamOptions &&o) noexcept : _opts(std::move(o)) {
        _iterators.reserve(1U);
        _segments.reserve(1U + (_opts.maximum_size_bytes - 1U) / _opts.minimum_segment_size_bytes);
    }

    StreamError removeSegmentsIfNewRecordBeyondMaxSize(uint32_t record_size) noexcept;

    StreamError makeNextSegment() noexcept;
    StreamError loadExistingSegments() noexcept;

  public:
    static expected<std::shared_ptr<FileStream>, StreamError> openOrCreate(StreamOptions &&) noexcept;

    virtual expected<uint64_t, StreamError> append(BorrowedSlice, const AppendOptions &) noexcept override;
    virtual expected<uint64_t, StreamError> append(OwnedSlice &&, const AppendOptions &) noexcept override;

    virtual expected<OwnedRecord, StreamError> read(uint64_t, const ReadOptions &) const noexcept override;

    virtual Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept override;
    virtual StreamError deleteIterator(const std::string &identifier) noexcept override;

    virtual StreamError setCheckpoint(const std::string &, uint64_t) noexcept override;
};

} // namespace gg
} // namespace aws