#include "fileDb.hpp"
#include "crc32.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <iomanip>
#include <sstream>
#include <string>

namespace aws::gg {
static constexpr const int UINT64_MAX_DECIMAL_COUNT = 19;

// NOLINTBEGIN
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
        h = (((h & 0xff000000u) >> 24) | ((h & 0x00ff0000u) >> 8) | ((h & 0x0000ff00u) << 8) |
             ((h & 0x000000ffu) << 24));
    }
    return h;
}
constexpr auto _ntohl(std::uint32_t h) { return _htonl(h); }
static_assert(htonl(0xFFEE) == _htonl(0xFFEE));
// NOLINTEND

constexpr size_t HEADER_SIZE = 32;
constexpr int32_t MAGIC_BYTES = 0xAAAAAA;
constexpr uint8_t VERSION = 0x01;
constexpr uint32_t MAGIC_AND_VERSION = MAGIC_BYTES << 8 | VERSION;

#pragma pack(push, 4)
struct LogEntryHeader {
    int32_t magic_and_version = _htonl(MAGIC_AND_VERSION);
    int32_t relative_sequence_number = 0;
    int32_t byte_position = 0;
    int64_t crc = 0;
    int64_t timestamp = 0;
    int32_t payload_length_bytes = 0;
};
#pragma pack(pop)

static_assert(sizeof(LogEntryHeader) == HEADER_SIZE, "Header size must be 32 bytes!");

std::shared_ptr<StreamInterface> FileStream::openOrCreate(StreamOptions &&opts) {
    return std::shared_ptr<StreamInterface>(new FileStream(std::move(opts)));
}

void FileStream::loadExistingSegments() {
    auto files = _opts.file_implementation->list();
    for (const auto &f : files) {
        auto idx = f.find_last_of(".log");
        if (idx != std::string::npos) {
            auto base = std::stoull(std::string{f.substr(0, idx - 3)});
            _segments.emplace_back(base, _opts.file_implementation);
        }
    }

    std::sort(_segments.begin(), _segments.end());

    // Setup our internal state
    if (!_segments.empty()) {
        _next_sequence_number = _segments.back().getHighestSeqNum() + 1;
        _first_sequence_number = _segments.front().getBaseSeqNum();
        uint64_t size = 0;
        for (const auto &s : _segments) {
            size += s.totalSizeBytes();
        }
        _current_size_bytes = size;
    }
}

FileSegment::FileSegment(uint64_t base, std::shared_ptr<FileSystemInterface> interface)
    : _file_implementation(std::move(interface)), _base_seq_num(base), _highest_seq_num(base) {
    std::ostringstream oss;
    oss << std::setw(UINT64_MAX_DECIMAL_COUNT) << std::setfill('0') << _base_seq_num << ".log";

    _segment_id = oss.str();
    _f = _file_implementation->open(_segment_id);

    size_t offset = 0;
    try {
        while (true) {
            const auto headerData = _f->read(offset, offset + HEADER_SIZE);
            LogEntryHeader const *header = convertSliceToHeader(headerData);

            // TODO: Do something if corrupted
            if (header->magic_and_version != MAGIC_AND_VERSION) {
                throw std::runtime_error("Invalid magic bytes");
            }

            offset += HEADER_SIZE;
            offset += header->payload_length_bytes;
            _total_bytes += header->payload_length_bytes + HEADER_SIZE;
            _highest_seq_num = std::max(_highest_seq_num.load(), _base_seq_num + header->relative_sequence_number);
        }
    } catch (std::runtime_error &e) {
        if (strncmp(e.what(), "EOF", 3) != 0) {
            throw e;
        }
    }
}

LogEntryHeader const *FileSegment::convertSliceToHeader(const OwnedSlice &data) {
    auto *header =
        reinterpret_cast<LogEntryHeader *>(data.data()); // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    header->payload_length_bytes = static_cast<int32_t>(_ntohl(header->payload_length_bytes));
    header->relative_sequence_number = static_cast<int32_t>(_ntohl(header->relative_sequence_number));
    header->byte_position = static_cast<int32_t>(_ntohl(header->byte_position));
    header->crc = static_cast<int64_t>(_ntohll(header->crc));
    header->timestamp = static_cast<int64_t>(_ntohll(header->timestamp));
    header->magic_and_version = static_cast<int32_t>(_ntohl(header->magic_and_version));
    return header;
}

void FileSegment::append(BorrowedSlice d, int64_t timestamp_ms, uint64_t sequence_number) {
    auto ts = static_cast<int64_t>(_htonll(timestamp_ms));
    auto data_len_swap = static_cast<int32_t>(_htonl(d.size()));
    auto header = LogEntryHeader{
        .relative_sequence_number = static_cast<int32_t>(_htonl((sequence_number - _base_seq_num))),
        .byte_position = static_cast<int32_t>(_htonl(_total_bytes.fetch_add(d.size() + sizeof(LogEntryHeader)))),
        .crc = static_cast<int64_t>(_htonll(
            crc32::update(crc32::update(crc32::update(0, &ts, sizeof(int64_t)), &data_len_swap, sizeof(int32_t)),
                          d.data(), d.size()))),
        .timestamp = ts,
        .payload_length_bytes = static_cast<int32_t>(_htonl(d.size())),
    };

    // TODO: Handle errors and rollback changes to internal state if required (eg. _total_bytes)
    _f->append(
        BorrowedSlice{reinterpret_cast<const uint8_t *>(&header), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(header)});
    _f->append(d);

    _highest_seq_num = std::max(_highest_seq_num.load(), sequence_number);
}

OwnedRecord FileSegment::read(uint64_t sequence_number, uint64_t suggested_start) const {
    return getRecord(sequence_number, suggested_start, suggested_start != 0);
}

OwnedRecord FileSegment::getRecord(uint64_t sequence_number, size_t offset, bool suggested_start) const {
    // We will try to find the record by reading the segment starting at the offset.
    // If a suggested starting position within the segment was suggested to us, we start from further into the file.
    // If any error occurs with the suggested starting point, we will restart from the beginning of the file.
    // Any failures during reading without a suggested started point are raised immediately.

    while (true) {
        try {
            const auto headerData = _f->read(offset, offset + HEADER_SIZE);
            LogEntryHeader const *header = convertSliceToHeader(headerData);

            if (header->magic_and_version != MAGIC_AND_VERSION) {
                throw std::runtime_error("Invalid magic bytes");
            }

            uint64_t expected_rel_seq_num = sequence_number - _base_seq_num;
            if (header->relative_sequence_number > expected_rel_seq_num) {
                throw std::runtime_error("Record not found");
            }

            // We found the one we want!
            if (header->relative_sequence_number == expected_rel_seq_num) {
                auto data = _f->read(offset + HEADER_SIZE, offset + HEADER_SIZE + header->payload_length_bytes);
                auto data_len_swap = static_cast<int32_t>(_htonl(header->payload_length_bytes));
                if (auto ts_swap = static_cast<int64_t>(_htonll(header->timestamp));
                    header->crc !=
                    static_cast<int64_t>(crc32::update(
                        crc32::update(crc32::update(0, &ts_swap, sizeof(int64_t)), &data_len_swap, sizeof(int32_t)),
                        data.data(), data.size()))) {
                    throw std::runtime_error("CRC does not match. Record corrupted");
                }

                return OwnedRecord{
                    std::move(data),
                    header->timestamp,
                    sequence_number,
                    offset + HEADER_SIZE,
                };
            }

            offset += HEADER_SIZE;
            offset += header->payload_length_bytes;
        } catch (std::exception &e) {
            if (suggested_start) {
                offset = 0;
                suggested_start = false;
                continue;
            }
            throw e;
        }
    }
}

void FileSegment::remove() {
    // Close file handle, then delete file
    _f.reset(nullptr);
    _file_implementation->remove(_segment_id);
}

void FileStream::makeNextSegment() { _segments.emplace_back(_next_sequence_number, _opts.file_implementation); }

uint64_t FileStream::append(BorrowedSlice d) {
    auto record_size = d.size();
    removeSegmentsIfNewRecordBeyondMaxSize(record_size);

    if (_segments.empty()) {
        makeNextSegment();
    }
    if (auto const &last_segment = _segments.back();
        last_segment.totalSizeBytes() >= _opts.minimum_segment_size_bytes) {
        makeNextSegment();
    }

    auto seq = _next_sequence_number++;
    auto timestamp = aws::gg::timestamp();

    _segments.back().append(d, timestamp, seq);
    _current_size_bytes += record_size;

    return seq;
}

void FileStream::removeSegmentsIfNewRecordBeyondMaxSize(size_t record_size) {
    if (record_size > _opts.maximum_db_size_bytes) {
        throw std::runtime_error("Record too large");
    }

    // Make room if we need more room
    while (_current_size_bytes + record_size > _opts.maximum_db_size_bytes) {
        auto &to_delete = _segments.front();
        _current_size_bytes -= to_delete.totalSizeBytes();
        to_delete.remove();
        // Remove from in-memory
        _segments.erase(_segments.begin());
        _first_sequence_number = _segments.front().getBaseSeqNum();
    }
}

uint64_t FileStream::append(OwnedSlice &&d) {
    auto x = std::move(d);
    return append(BorrowedSlice(x.data(), x.size()));
}

static constexpr const char *const RecordNotFoundErrorStr = "Record not found";

[[nodiscard]] OwnedRecord FileStream::read(uint64_t sequence_number, uint64_t suggested_start) const {
    if (sequence_number < _first_sequence_number) {
        throw std::runtime_error(RecordNotFoundErrorStr);
    }
    if (sequence_number >= _next_sequence_number) {
        throw std::runtime_error(RecordNotFoundErrorStr);
    }

    for (const auto &seg : _segments) {
        if (sequence_number >= seg.getBaseSeqNum() && sequence_number <= seg.getHighestSeqNum()) {
            return seg.read(sequence_number, suggested_start);
        }
    }

    throw std::runtime_error(RecordNotFoundErrorStr);
}

[[nodiscard]] Iterator FileStream::openOrCreateIterator(char identifier, IteratorOptions) {
    return Iterator{WEAK_FROM_THIS(), identifier,
                    _iterators.count(identifier) ? _iterators[identifier] : _first_sequence_number};
}

void FileStream::deleteIterator(char identifier) { _iterators.erase(identifier); }

void FileStream::setCheckpoint(char identifier, uint64_t sequence_number) { _iterators[identifier] = sequence_number; }

}; // namespace aws::gg
