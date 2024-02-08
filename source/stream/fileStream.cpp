#include "stream/fileStream.hpp"
#include "common/crc32.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>

namespace aws {
namespace gg {
static constexpr const int UINT64_MAX_DECIMAL_COUNT = 19;

// NOLINTBEGIN
constexpr auto _htonll(std::uint64_t h) {
    if (htonl(0xFFFF0000) != 0xFFFF0000) {
        static_assert(CHAR_BIT == 8, "Char must be 8 bits");
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
// NOLINTEND

constexpr size_t HEADER_SIZE = 32;
constexpr int32_t MAGIC_BYTES = 0xAAAAAA;
constexpr uint8_t VERSION = 0x01;
constexpr int32_t MAGIC_AND_VERSION = MAGIC_BYTES << 8 | VERSION;

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

static constexpr const char *const RecordNotFoundErrorStr = "Record not found";

expected<std::shared_ptr<StreamInterface>, FileError> FileStream::openOrCreate(StreamOptions &&opts) {
    auto stream = std::shared_ptr<FileStream>(new FileStream(std::move(opts)));
    auto err = stream->loadExistingSegments();
    if (err.code != FileErrorCode::NoError) {
        return err;
    }
    return std::shared_ptr<StreamInterface>(stream);
}

FileError FileStream::loadExistingSegments() {
    auto kv_err_or = KV::openOrCreate(std::move(_opts.kv_options));
    if (!kv_err_or) {
        // TODO: Better error mapping
        return FileError{FileErrorCode::Unknown, kv_err_or.err().msg};
    }
    _kv_store = std::move(kv_err_or.val());

    auto files_or = _opts.file_implementation->list();
    if (!files_or) {
        return files_or.err();
    }

    auto files = std::move(files_or.val());
    for (const auto &f : files) {
        auto idx = f.rfind(".log");
        if (idx != std::string::npos) {
            auto base = std::stoull(std::string{f.substr(0, idx)});
            FileSegment segment{base, _opts.file_implementation};
            auto err = segment.open();
            if (err.code != FileErrorCode::NoError) {
                return err;
            }
            _segments.push_back(std::move(segment));
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

    return FileError{FileErrorCode::NoError, {}};
}

FileSegment::FileSegment(uint64_t base, std::shared_ptr<FileSystemInterface> interface)
    : _file_implementation(std::move(interface)), _base_seq_num(base), _highest_seq_num(base) {
    std::ostringstream oss;
    oss << std::setw(UINT64_MAX_DECIMAL_COUNT) << std::setfill('0') << _base_seq_num << ".log";

    _segment_id = oss.str();
}

FileError FileSegment::open() {
    auto file_or = _file_implementation->open(_segment_id);
    if (file_or) {
        _f = std::move(file_or.val());
    }

    size_t offset = 0;

    while (true) {
        const auto header_data_or = _f->read(offset, offset + HEADER_SIZE);
        if (!header_data_or) {
            if (header_data_or.err().code == FileErrorCode::EndOfFile) {
                return FileError{FileErrorCode::NoError, {}};
            }
            return header_data_or.err();
        }
        LogEntryHeader const *header = convertSliceToHeader(header_data_or.val());

        // TODO: Do something if corrupted
        if (header->magic_and_version != MAGIC_AND_VERSION) {
            return FileError{FileErrorCode::Unknown, "Magic bytes and version does not match expected value"};
        }
        // TODO: Add option to check the CRC of the data too

        offset += HEADER_SIZE;
        offset += header->payload_length_bytes;
        _total_bytes += header->payload_length_bytes + HEADER_SIZE;
        _highest_seq_num = std::max(_highest_seq_num.load(), _base_seq_num + header->relative_sequence_number);
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

expected<OwnedRecord, StreamError> FileSegment::read(uint64_t sequence_number, uint64_t suggested_start) const {
    return getRecord(sequence_number, suggested_start, suggested_start != 0);
}

expected<OwnedRecord, StreamError> FileSegment::getRecord(uint64_t sequence_number, size_t offset,
                                                          bool suggested_start) const {
    // We will try to find the record by reading the segment starting at the offset.
    // If a suggested starting position within the segment was suggested to us, we start from further into the file.
    // If any error occurs with the suggested starting point, we will restart from the beginning of the file.
    // Any failures during reading without a suggested started point are raised immediately.

    while (true) {
        auto header_data_or = _f->read(offset, offset + HEADER_SIZE);
        if (!header_data_or) {
            if (suggested_start) {
                offset = 0;
                suggested_start = false;
                continue;
            }
            return StreamError{StreamErrorCode::ReadError, header_data_or.err().msg};
        }
        LogEntryHeader const *header = convertSliceToHeader(header_data_or.val());

        if (header->magic_and_version != MAGIC_AND_VERSION) {
            return StreamError{StreamErrorCode::HeaderDataCorrupted, {}};
        }

        auto expected_rel_seq_num = static_cast<int32_t>(sequence_number - _base_seq_num);
        if (header->relative_sequence_number > expected_rel_seq_num) {
            return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
        }

        // We found the one we want!
        if (header->relative_sequence_number == expected_rel_seq_num) {
            auto data_or = _f->read(offset + HEADER_SIZE, offset + HEADER_SIZE + header->payload_length_bytes);
            if (!data_or) {
                return StreamError{StreamErrorCode::ReadError, header_data_or.err().msg};
            }
            auto data = std::move(data_or.val());
            auto data_len_swap = static_cast<int32_t>(_htonl(header->payload_length_bytes));
            auto ts_swap = static_cast<int64_t>(_htonll(header->timestamp));
            if (header->crc !=
                static_cast<int64_t>(crc32::update(
                    crc32::update(crc32::update(0, &ts_swap, sizeof(int64_t)), &data_len_swap, sizeof(int32_t)),
                    data.data(), data.size()))) {
                return StreamError{StreamErrorCode::RecordDataCorrupted, {}};
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
    }
}

void FileSegment::remove() {
    // Close file handle, then delete file
    _f.reset(nullptr);
    _file_implementation->remove(_segment_id);
}

FileError FileStream::makeNextSegment() {
    FileSegment segment{_next_sequence_number, _opts.file_implementation};

    auto err = segment.open();
    if (err.code != FileErrorCode::NoError) {
        return err;
    }

    _segments.push_back(std::move(segment));
    return FileError{FileErrorCode::NoError, {}};
}

expected<uint64_t, StreamError> FileStream::append(BorrowedSlice d) {
    auto record_size = d.size();
    {
        auto err = removeSegmentsIfNewRecordBeyondMaxSize(record_size);
        if (err.code != StreamErrorCode::NoError) {
            return err;
        }
    }

    if (_segments.empty()) {
        auto err = makeNextSegment();
        if (err.code != FileErrorCode::NoError) {
            return StreamError{StreamErrorCode::WriteError, err.msg};
        }
    }

    auto const &last_segment = _segments.back();
    if (last_segment.totalSizeBytes() >= _opts.minimum_segment_size_bytes) {
        auto err = makeNextSegment();
        if (err.code != FileErrorCode::NoError) {
            return StreamError{StreamErrorCode::WriteError, err.msg};
        }
    }

    auto seq = _next_sequence_number++;
    auto timestamp = aws::gg::timestamp();

    _segments.back().append(d, timestamp, seq);
    _current_size_bytes += record_size;

    return seq;
}

StreamError FileStream::removeSegmentsIfNewRecordBeyondMaxSize(size_t record_size) {
    if (record_size > _opts.maximum_size_bytes) {
        return StreamError{StreamErrorCode::RecordTooLarge, {}};
    }

    // Make room if we need more room
    while (_current_size_bytes + record_size > _opts.maximum_size_bytes) {
        auto &to_delete = _segments.front();
        _current_size_bytes -= to_delete.totalSizeBytes();
        to_delete.remove();
        // Remove from in-memory
        _segments.erase(_segments.begin());
        _first_sequence_number = _segments.front().getBaseSeqNum();
    }
    return StreamError{StreamErrorCode::NoError, {}};
}

expected<uint64_t, StreamError> FileStream::append(OwnedSlice &&d) {
    auto x = std::move(d);
    return append(BorrowedSlice(x.data(), x.size()));
}

[[nodiscard]] expected<OwnedRecord, StreamError> FileStream::read(uint64_t sequence_number,
                                                                  uint64_t suggested_start) const {
    if (sequence_number < _first_sequence_number || sequence_number >= _next_sequence_number) {
        return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
    }

    for (const auto &seg : _segments) {
        if (sequence_number >= seg.getBaseSeqNum() && sequence_number <= seg.getHighestSeqNum()) {
            return seg.read(sequence_number, suggested_start);
        }
    }

    return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
}

[[nodiscard]] Iterator FileStream::openOrCreateIterator(const std::string &identifier, IteratorOptions) {
    for (const auto &iter : _iterators) {
        if (iter.getIdentifier() == identifier) {
            return Iterator{WEAK_FROM_THIS(), identifier, std::max(_first_sequence_number, iter.getSequenceNumber())};
        }
    }

    _iterators.emplace_back(identifier, _first_sequence_number, _kv_store);
    return Iterator{WEAK_FROM_THIS(), identifier,
                    std::max(_first_sequence_number, _iterators.back().getSequenceNumber())};
}

void FileStream::deleteIterator(const std::string &identifier) {
    for (size_t i = 0; i < _iterators.size(); i++) {
        auto iter = _iterators[i];
        if (iter.getIdentifier() == identifier) {
            iter.remove();
            auto it = _iterators.begin();
            std::advance(it, i);
            _iterators.erase(it);
            break;
        }
    }
}

void FileStream::setCheckpoint(const std::string &identifier, uint64_t sequence_number) {
    // TODO: if no identifier in map
    for (auto &iter : _iterators) {
        if (iter.getIdentifier() == identifier) {
            iter.setCheckpoint(sequence_number);
            return;
        }
    }
}

PersistentIterator::PersistentIterator(std::string id, uint64_t start, std::shared_ptr<KV> kv)
    : _id(std::move(id)), _store(std::move(kv)), _sequence_number(start) {
    auto value_or = _store->get(_id);
    if (value_or) {
        auto last_value = *reinterpret_cast<uint64_t *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            value_or.val().data());
        _sequence_number = std::max(start, last_value);
    }
}

void PersistentIterator::setCheckpoint(uint64_t sequence_number) {
    // TODO: something with the error
    _sequence_number = sequence_number;
    [[maybe_unused]] auto _ = _store->put(
        _id, BorrowedSlice{reinterpret_cast<const uint8_t *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                               &sequence_number),
                           sizeof(uint64_t)});
}

void PersistentIterator::remove() {
    // TODO: something with the error
    [[maybe_unused]] auto _ = _store->remove(_id);
}

} // namespace gg
}; // namespace aws
