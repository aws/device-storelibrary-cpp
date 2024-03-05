#pragma once
#include "common/expected.hpp"
#include "common/logging.hpp"
#include "common/slices.hpp"
#include "filesystem/filesystem.hpp"
#include "kv/kv.hpp"
#include "stream.hpp"
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace aws {
namespace gg {

struct LogEntryHeader;
class FileSegment {
  public:
    FileSegment(const uint64_t base, std::shared_ptr<FileSystemInterface>, std::shared_ptr<logging::Logger>) noexcept;

    FileSegment(FileSegment &&s) = default;
    FileSegment &operator=(FileSegment &&s) = default;

    FileSegment(const FileSegment &) = delete;
    FileSegment &operator=(const FileSegment &) = delete;

    ~FileSegment() = default;

    StreamError open(const bool full_corruption_check_on_open) noexcept;

    bool operator<(const FileSegment &other) const noexcept {
        return _base_seq_num < other._base_seq_num;
    }

    expected<uint64_t, FileError> append(const BorrowedSlice d, const int64_t timestamp_ms,
                                         const uint64_t sequence_number, const bool sync) noexcept;

    expected<OwnedRecord, StreamError> read(const uint64_t sequence_number, const ReadOptions &) const noexcept;

    void remove() noexcept;

    std::uint64_t getBaseSeqNum() const noexcept {
        return _base_seq_num;
    }

    std::uint64_t getHighestSeqNum() const noexcept {
        return _highest_seq_num;
    }

    std::uint32_t totalSizeBytes() const noexcept {
        return _total_bytes;
    }

  private:
    std::unique_ptr<FileLike> _f;
    std::shared_ptr<FileSystemInterface> _file_implementation{};
    std::shared_ptr<logging::Logger> _logger;
    std::uint64_t _base_seq_num{1U};
    std::uint64_t _highest_seq_num{0U};
    std::uint32_t _total_bytes{0U};
    std::string _segment_id;

    static LogEntryHeader convertSliceToHeader(const OwnedSlice &) noexcept;

    void truncateAndLog(const uint32_t truncate, const StreamError &err) const noexcept;
};

class PersistentIterator {
  public:
    PersistentIterator(std::string id, const uint64_t start, std::shared_ptr<kv::KV>) noexcept;

    uint64_t getSequenceNumber() const noexcept {
        return _sequence_number;
    }
    const std::string &getIdentifier() const noexcept {
        return _id;
    }
    StreamError setCheckpoint(const uint64_t) noexcept;
    StreamError remove() const noexcept;

  private:
    std::string _id;
    std::shared_ptr<kv::KV> _store;
    uint64_t _sequence_number{0U};
};

class __attribute__((visibility("default"))) FileStream : public StreamInterface {
  private:
    mutable std::mutex _segments_lock{}; // TODO: would like this to be a shared_mutex, but that is c++17.
    StreamOptions _opts;
    std::shared_ptr<kv::KV> _kv_store{};
    std::vector<PersistentIterator> _iterators{};
    std::vector<FileSegment> _segments{};

    explicit FileStream(StreamOptions &&o) noexcept : _opts(std::move(o)) {
        _iterators.reserve(1U);
        const auto toReserve = 1U + (_opts.maximum_size_bytes - 1U) / _opts.minimum_segment_size_bytes;
        _segments.reserve(toReserve);
    }

    StreamError removeSegmentsIfNewRecordBeyondMaxSize(const uint32_t record_size,
                                                       const bool remove_oldest_segments_if_full) noexcept;
    StreamError makeNextSegment() noexcept;
    StreamError loadExistingSegments() noexcept;

  public:
    static expected<std::shared_ptr<FileStream>, StreamError> openOrCreate(StreamOptions &&) noexcept;

    expected<uint64_t, StreamError> append(const BorrowedSlice, const AppendOptions &) noexcept override;
    expected<uint64_t, StreamError> append(OwnedSlice &&, const AppendOptions &) noexcept override;

    expected<OwnedRecord, StreamError> read(const uint64_t, const ReadOptions &) const noexcept override;

    Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept override;
    StreamError deleteIterator(const std::string &identifier) noexcept override;

    StreamError setCheckpoint(const std::string &, const uint64_t) noexcept override;

    ~FileStream() override = default;
};

} // namespace gg
} // namespace aws