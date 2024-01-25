#pragma once
#include "db.hpp"
#include <climits>
#include <string>
#include <unordered_map>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    struct LogEntryHeader;
    class FileSegment {
      public:
        FileSegment(uint64_t base, std::shared_ptr<FileSystemInterface>);

        FileSegment(FileSegment &&s)
            : _f(std::move(s._f)), _file_implementation(std::move(s._file_implementation)),
              _base_seq_num(s._base_seq_num), _highest_seq_num(s._highest_seq_num.load()),
              _total_bytes(s._total_bytes.load()), _segment_id(std::move(s._segment_id)){};

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

        bool operator<(const FileSegment &other) const { return _base_seq_num < other._base_seq_num; }

        void append(BorrowedSlice d, int64_t timestamp_ms, uint64_t sequence_number);

        OwnedRecord read(uint64_t sequence_number, uint64_t suggested_start) const;

        void remove();

        std::uint64_t getBaseSeqNum() const { return _base_seq_num; }

        std::uint64_t getHighestSeqNum() const { return _highest_seq_num; }

        std::uint64_t totalSizeBytes() const { return _total_bytes; }

      private:
        std::unique_ptr<FileLike> _f;
        std::shared_ptr<FileSystemInterface> _file_implementation = {};
        std::uint64_t _base_seq_num = {1};
        std::atomic_uint64_t _highest_seq_num = {0};
        std::atomic_uint64_t _total_bytes = {0};
        std::string _segment_id;

        OwnedRecord getRecord(uint64_t sequence_number, size_t offset, bool suggested_start) const;

        static LogEntryHeader const *convertSliceToHeader(const OwnedSlice &);
    };

    class FileStream : public StreamInterface {
      private:
        StreamOptions _opts;
        std::unordered_map<char, uint64_t> _iterators = {};
        std::vector<FileSegment> _segments = {};

        explicit FileStream(StreamOptions &&o) : _opts(std::move(o)) { loadExistingSegments(); }

        void removeSegmentsIfNewRecordBeyondMaxSize(size_t record_size);

        void makeNextSegment();
        void loadExistingSegments();

      public:
        static std::shared_ptr<StreamInterface> openOrCreate(StreamOptions &&);

        uint64_t append(BorrowedSlice) override;
        uint64_t append(OwnedSlice &&) override;

        [[nodiscard]] OwnedRecord read(uint64_t sequence_number) const override { return read(sequence_number, 0); };
        [[nodiscard]] OwnedRecord read(uint64_t sequence_number, uint64_t suggested_start) const override;

        [[nodiscard]] Iterator openOrCreateIterator(char identifier, IteratorOptions) override;
        void deleteIterator(char identifier) override;

        void setCheckpoint(char, uint64_t) override;
    };

} // namespace gg
} // namespace aws