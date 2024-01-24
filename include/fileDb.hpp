#pragma once
#include "db.hpp"
#include <arpa/inet.h>
#include <climits>
#include <filesystem>
#include <unordered_map>

namespace aws::gg {
constexpr auto _htonll(std::uint64_t h) {
    if (htonl(0xFFFF0000) != 0xFFFF0000) {
        static_assert(CHAR_BIT == 8);
        constexpr auto shift_bytes1{8};
        constexpr auto shift_bytes2{16};
        constexpr auto shift_bytes4{32};
        h = ((h & UINT64_C(0x00FF00FF00FF00FF)) << shift_bytes1) | ((h & UINT64_C(0xFF00FF00FF00FF00)) >> shift_bytes1);
        h = ((h & UINT64_C(0x0000FFFF0000FFFF)) << shift_bytes2) | ((h & UINT64_C(0xFFFF0000FFFF0000)) >> shift_bytes2);
        h = ((h & UINT64_C(0x00000000FFFFFFFF)) << shift_bytes4) | ((h & UINT64_C(0xFFFFFFFF00000000)) >> shift_bytes4);
    }
    return h;
}

constexpr auto _ntohll(std::uint64_t h) { return _htonll(h); }

constexpr auto _htonl(std::uint32_t h) {
    if (htonl(0xFFFF0000) != 0xFFFF0000) {
        h = ((((h) & 0xff000000u) >> 24) | (((h) & 0x00ff0000u) >> 8) | (((h) & 0x0000ff00u) << 8) |
             (((h) & 0x000000ffu) << 24));
    }
    return h;
}
constexpr auto _ntohl(std::uint32_t h) { return _htonl(h); }

constexpr size_t HEADER_SIZE = 32;
constexpr int32_t MAGIC_BYTES = 0xAAAAAA;
constexpr uint8_t VERSION = 0x01;
constexpr uint32_t MAGIC_AND_VERSION = MAGIC_BYTES << 8 | VERSION;

#pragma pack(push, 4)
struct LogEntryHeader {
    int32_t magic_and_version = _htonl(MAGIC_AND_VERSION);
    int32_t relative_sequence_number;
    int32_t byte_position;
    int64_t crc;
    int64_t timestamp;
    int32_t payload_length_bytes;
};
#pragma pack(pop)

static_assert(sizeof(LogEntryHeader) == HEADER_SIZE, "Header size must be 32 bytes!");

class FileSegment {
  public:
    FileSegment(uint64_t base, std::shared_ptr<FileSystemInterface>);

    FileSegment(FileSegment &&s)
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
    std::filesystem::path _segment_id;

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

    [[nodiscard]] const OwnedRecord read(uint64_t sequence_number) const override { return read(sequence_number, 0); };
    [[nodiscard]] const OwnedRecord read(uint64_t sequence_number, uint64_t suggested_start) const override;

    [[nodiscard]] Iterator openOrCreateIterator(char identifier, IteratorOptions) override;
    void deleteIterator(char identifier) override;

    void setCheckpoint(char, uint64_t) override;
};

class PosixFileLike : public FileLike {
    std::filesystem::path _path;
    FILE *_f = nullptr;

  public:
    PosixFileLike(std::filesystem::path &&path) : _path(std::move(path)) {
        _f = std::fopen(_path.c_str(), "ab+");
        if (!_f) {
            throw std::runtime_error(std::string{"Cannot open file "} + std::strerror(errno));
        }
    };

    OwnedSlice read(size_t begin, size_t end) override {
        if (end <= begin) {
            throw std::runtime_error("End must be after the beginning");
        }
        clearerr(_f);
        auto d = OwnedSlice{(end - begin)};
        if (std::fseek(_f, begin, SEEK_SET) != 0) {
            throw std::runtime_error(std::strerror(errno));
        }
        auto freadOut = std::fread((void *)d.data(), d.size(), 1, _f);
        if (freadOut != 1) {
            if (feof(_f) != 0) {
                throw std::runtime_error("EOF");
            } else {
                throw std::runtime_error(std::strerror(errno));
            }
        }
        return d;
    };

    void append(BorrowedSlice data) override {
        if (fwrite(data.data(), data.size(), 1, _f) != 1) {
            throw std::runtime_error(std::strerror(errno));
        }
    };

    void flush() override { fflush(_f); }

    ~PosixFileLike() override {
        if (_f) {
            std::fclose(_f);
        }
    }
};

class PosixFileSystem : public FileSystemInterface {
  private:
    std::filesystem::path _base_path;

  public:
    PosixFileSystem(std::filesystem::path &&base_path) : _base_path(std::move(base_path)) {
        std::filesystem::create_directories(_base_path);
    };

    std::unique_ptr<FileLike> open(std::string_view identifier) override {
        return std::make_unique<PosixFileLike>(_base_path / identifier);
    };

    bool exists(std::string_view identifier) override { return std::filesystem::exists(_base_path / identifier); };

    void rename(std::string_view old_id, std::string_view new_id) override {
        std::filesystem::rename(_base_path / old_id, _base_path / new_id);
    };

    void remove(std::string_view id) override { std::filesystem::remove(_base_path / id); };

    std::vector<std::string> list() override {
        std::vector<std::string> output;
        for (const auto &entry : std::filesystem::directory_iterator(_base_path)) {
            output.emplace_back(entry.path().filename().string());
        }
        return output;
    };
};
} // namespace aws::gg