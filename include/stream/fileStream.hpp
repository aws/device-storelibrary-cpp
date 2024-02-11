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

    [[nodiscard]] StreamError open(bool full_corruption_check_on_open) noexcept;

    bool operator<(const FileSegment &other) const noexcept { return _base_seq_num < other._base_seq_num; }

    [[nodiscard]] expected<uint64_t, FileError> append(BorrowedSlice d, int64_t timestamp_ms,
                                                       uint64_t sequence_number) noexcept;

    [[nodiscard]] expected<OwnedRecord, StreamError> read(uint64_t sequence_number, const ReadOptions &) const noexcept;

    void remove() noexcept;

    std::uint64_t getBaseSeqNum() const noexcept { return _base_seq_num; }

    std::uint64_t getHighestSeqNum() const noexcept { return _highest_seq_num; }

    std::uint64_t totalSizeBytes() const noexcept { return _total_bytes; }

  private:
    std::unique_ptr<FileLike> _f;
    std::shared_ptr<FileSystemInterface> _file_implementation{};
    std::shared_ptr<logging::Logger> _logger;
    std::uint64_t _base_seq_num{1};
    std::uint64_t _highest_seq_num{0};
    std::uint64_t _total_bytes{0};
    std::string _segment_id;

    static LogEntryHeader const *convertSliceToHeader(const OwnedSlice &) noexcept;
};

class PersistentIterator {
  public:
    PersistentIterator(std::string id, uint64_t start, std::shared_ptr<kv::KV>) noexcept;

    uint64_t getSequenceNumber() const noexcept { return _sequence_number; }
    const std::string &getIdentifier() const noexcept { return _id; }
    void setCheckpoint(uint64_t) noexcept;
    void remove() noexcept;

  private:
    std::string _id;
    std::shared_ptr<kv::KV> _store;
    uint64_t _sequence_number{0};
};

class __attribute__((visibility("default"))) FileStream : public StreamInterface {
  private:
    mutable std::mutex _segments_lock{}; // TODO: would like this to be a shared_mutex, but that is c++17.
    StreamOptions _opts;
    std::shared_ptr<kv::KV> _kv_store{};
    std::vector<PersistentIterator> _iterators{};
    std::vector<FileSegment> _segments{};

    explicit FileStream(StreamOptions &&o) noexcept : _opts(std::move(o)) {
        _iterators.reserve(1);
        _segments.reserve(1 + ((_opts.maximum_size_bytes - 1) / _opts.minimum_segment_size_bytes));
    }

    [[nodiscard]] StreamError removeSegmentsIfNewRecordBeyondMaxSize(size_t record_size) noexcept;

    [[nodiscard]] StreamError makeNextSegment() noexcept;
    [[nodiscard]] StreamError loadExistingSegments() noexcept;

  public:
    [[nodiscard]] static expected<std::shared_ptr<StreamInterface>, StreamError>
    openOrCreate(StreamOptions &&) noexcept;

    expected<uint64_t, StreamError> append(BorrowedSlice) noexcept override;
    expected<uint64_t, StreamError> append(OwnedSlice &&) noexcept override;

    [[nodiscard]] expected<OwnedRecord, StreamError> read(uint64_t, const ReadOptions &) const noexcept override;

    [[nodiscard]] Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept override;
    void deleteIterator(const std::string &identifier) noexcept override;

    void setCheckpoint(const std::string &, uint64_t) noexcept override;
};

} // namespace gg
} // namespace aws