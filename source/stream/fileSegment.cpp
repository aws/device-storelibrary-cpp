#include "common/crc32.hpp"
#include "stream/fileStream.hpp"
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>

namespace aws {
namespace gg {

static constexpr const int UINT64_MAX_DECIMAL_COUNT = 19;

#define IS_LITTLE_ENDIAN (*(uint16_t *)"\0\1" >> 8)

// NOLINTBEGIN
auto _htonll(std::uint64_t h) {
    if (IS_LITTLE_ENDIAN) {
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

auto _ntohll(std::uint64_t h) { return _htonll(h); }

auto _htonl(std::uint32_t h) {
    if (IS_LITTLE_ENDIAN) {
        h = (((h & 0xff000000u) >> 24) | ((h & 0x00ff0000u) >> 8) | ((h & 0x0000ff00u) << 8) |
             ((h & 0x000000ffu) << 24));
    }
    return h;
}
auto _ntohl(std::uint32_t h) { return _htonl(h); }
// NOLINTEND

constexpr uint8_t HEADER_SIZE = 32;
constexpr int32_t MAGIC_BYTES = 0xAAAAAA;
constexpr uint8_t VERSION = 0x01;
constexpr int32_t MAGIC_AND_VERSION = MAGIC_BYTES << 8 | VERSION;

#pragma pack(push, 4)
struct LogEntryHeader {
    int32_t magic_and_version = static_cast<int32_t>(_htonl(MAGIC_AND_VERSION));
    int32_t relative_sequence_number = 0;
    int32_t byte_position = 0;
    int64_t crc = 0;
    int64_t timestamp = 0;
    int32_t payload_length_bytes = 0;
};
#pragma pack(pop)

static_assert(sizeof(LogEntryHeader) == HEADER_SIZE, "Header size must be 32 bytes!");

static std::string string(const StreamErrorCode e) {
    using namespace std::string_literals;
    switch (e) {
    case StreamErrorCode::NoError:
        return "NoError"s;
    case StreamErrorCode::RecordNotFound:
        return "RecordNotFound"s;
    case StreamErrorCode::RecordDataCorrupted:
        return "RecordDataCorrupted"s;
    case StreamErrorCode::HeaderDataCorrupted:
        return "HeaderDataCorrupted"s;
    case StreamErrorCode::RecordTooLarge:
        return "RecordTooLarge"s;
    case StreamErrorCode::ReadError:
        return "ReadError"s;
    case StreamErrorCode::WriteError:
        return "WriteError"s;
    case StreamErrorCode::StreamClosed:
        return "StreamClosed"s;
    case StreamErrorCode::InvalidArguments:
        return "InvalidArguments"s;
    case StreamErrorCode::Unknown:
        return "Unknown"s;
    }
    // Unreachable.
    return {};
}

FileSegment::FileSegment(uint64_t base, std::shared_ptr<FileSystemInterface> interface,
                         std::shared_ptr<logging::Logger> logger)
    : _file_implementation(std::move(interface)), _logger(std::move(logger)), _base_seq_num(base),
      _highest_seq_num(base) {
    std::ostringstream oss;
    oss << std::setw(UINT64_MAX_DECIMAL_COUNT) << std::setfill('0') << _base_seq_num << ".log";

    _segment_id = oss.str();
}

void FileSegment::truncateAndLog(uint32_t truncate, const StreamError &err) const noexcept {
    if (_logger && _logger->level >= logging::LogLevel::Warning) {
        using namespace std::string_literals;
        auto message = "Truncating "s + _segment_id + " to a length of "s + std::to_string(truncate);
        if (!err.msg.empty()) {
            message += " because "s + err.msg;
        } else {
            message += " because "s + string(err.code);
        }
        _logger->log(logging::LogLevel::Warning, message);
    }
    _f->truncate(truncate);
}

StreamError FileSegment::open(bool full_corruption_check_on_open) {
    auto file_or = _file_implementation->open(_segment_id);
    if (file_or) {
        _f = std::move(file_or.val());
    }

    uint32_t offset = 0;

    while (true) {
        const auto header_data_or = _f->read(offset, offset + HEADER_SIZE);
        if (!header_data_or) {
            if (header_data_or.err().code == FileErrorCode::EndOfFile) {
                // If we reached the end of the file, there could have been extra data at the end, but less
                // than what we were hoping to read. Truncate the file now so that everything before this point
                // is known valid and everything after is gone.
                _f->truncate(offset);
                return StreamError{StreamErrorCode::NoError, {}};
            }

            truncateAndLog(offset, StreamError{StreamErrorCode::ReadError, header_data_or.err().msg});
            continue;
        }
        LogEntryHeader const *header = convertSliceToHeader(header_data_or.val());

        if (header->magic_and_version != MAGIC_AND_VERSION) {
            truncateAndLog(offset, StreamError{StreamErrorCode::HeaderDataCorrupted, {}});
            continue;
        }
        if (full_corruption_check_on_open) {
            auto value_or =
                read(header->relative_sequence_number + _base_seq_num, ReadOptions{
                                                                           .check_for_corruption = true,
                                                                           .may_return_later_records = false,
                                                                           .suggested_start = offset,
                                                                       });
            if (!value_or) {
                truncateAndLog(offset, value_or.err());
                continue;
            }
        }

        offset += HEADER_SIZE;
        offset += header->payload_length_bytes;
        _total_bytes += header->payload_length_bytes + HEADER_SIZE;
        _highest_seq_num = std::max(_highest_seq_num, _base_seq_num + header->relative_sequence_number);
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

expected<uint64_t, FileError> FileSegment::append(BorrowedSlice d, int64_t timestamp_ms, uint64_t sequence_number) {
    auto ts = static_cast<int64_t>(_htonll(timestamp_ms));
    auto data_len_swap = static_cast<int32_t>(_htonl(d.size()));
    auto byte_position = static_cast<int32_t>(_htonl(_total_bytes));

    auto crc = static_cast<int64_t>(_htonll(
        crc32::crc32_of(BorrowedSlice{&ts, sizeof(ts)}, BorrowedSlice{&data_len_swap, sizeof(data_len_swap)}, d)));
    auto header = LogEntryHeader{
        .relative_sequence_number = static_cast<int32_t>(_htonl(static_cast<int32_t>(sequence_number - _base_seq_num))),
        .byte_position = byte_position,
        .crc = crc,
        .timestamp = ts,
        .payload_length_bytes = static_cast<int32_t>(_htonl(d.size())),
    };

    // If an error happens when appending, truncate the file to the current size so that we don't have any
    // partial data in the file, and then return the error.
    auto e = _f->append(
        BorrowedSlice{reinterpret_cast<const uint8_t *>(&header), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(header)});
    if (e.code != FileErrorCode::NoError) {
        _f->truncate(_total_bytes);
        return e;
    }
    e = _f->append(d);
    if (e.code != FileErrorCode::NoError) {
        _f->truncate(_total_bytes);
        return e;
    }

    _highest_seq_num = std::max(_highest_seq_num, sequence_number);
    _total_bytes += d.size() + sizeof(LogEntryHeader);

    return d.size() + sizeof(LogEntryHeader);
}

expected<OwnedRecord, StreamError> FileSegment::read(uint64_t sequence_number, const ReadOptions &read_options) const {
    // We will try to find the record by reading the segment starting at the offset.
    // If a suggested starting position within the segment was suggested to us, we start from further into the file.
    // If any error occurs with the suggested starting point, we will restart from the beginning of the file.
    // Any failures during reading without a suggested started point are raised immediately.

    auto offset = read_options.suggested_start;
    auto suggested_start = offset != 0;

    while (true) {
        auto header_data_or = _f->read(offset, offset + HEADER_SIZE);
        if (!header_data_or) {
            if (suggested_start) {
                offset = 0;
                suggested_start = false;
                continue;
            }
            if (header_data_or.err().code == FileErrorCode::EndOfFile) {
                return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
            }
            return StreamError{StreamErrorCode::ReadError, header_data_or.err().msg};
        }
        LogEntryHeader const *header = convertSliceToHeader(header_data_or.val());

        if (header->magic_and_version != MAGIC_AND_VERSION) {
            return StreamError{StreamErrorCode::HeaderDataCorrupted, {}};
        }

        auto expected_rel_seq_num = static_cast<int32_t>(sequence_number - _base_seq_num);

        // If the record we read is after the one we wanted, and we're not allowed to return later records, we must fail
        if (header->relative_sequence_number > expected_rel_seq_num && !read_options.may_return_later_records) {
            return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
        }

        // We found the one we want, or the next available sequence number was acceptable to us
        if (header->relative_sequence_number == expected_rel_seq_num ||
            (header->relative_sequence_number > expected_rel_seq_num && read_options.may_return_later_records)) {
            auto data_or = _f->read(offset + HEADER_SIZE, offset + HEADER_SIZE + header->payload_length_bytes);
            if (!data_or) {
                return StreamError{StreamErrorCode::ReadError, header_data_or.err().msg};
            }
            auto data = std::move(data_or.val());
            auto data_len_swap = static_cast<int32_t>(_htonl(header->payload_length_bytes));
            auto ts_swap = static_cast<int64_t>(_htonll(header->timestamp));

            if (read_options.check_for_corruption &&
                header->crc !=
                    static_cast<int64_t>(crc32::crc32_of(BorrowedSlice{&ts_swap, sizeof(ts_swap)},
                                                         BorrowedSlice{&data_len_swap, sizeof(data_len_swap)},
                                                         BorrowedSlice{data.data(), data.size()}))) {
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
} // namespace gg
} // namespace aws