#include "aws/store/common/crc32.hpp"
#include "aws/store/common/expected.hpp"
#include "aws/store/common/logging.hpp"
#include "aws/store/common/slices.hpp"
#include "aws/store/common/util.hpp"
#include "aws/store/filesystem/filesystem.hpp"
#include "aws/store/stream/fileStream.hpp"
#include "aws/store/stream/stream.hpp"
#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>

// coverity[misra_cpp_2008_rule_2_13_2_violation] need to check for edianness
// coverity[autosar_cpp14_m2_13_2_violation] need to check for edianness
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define IS_LITTLE_ENDIAN (*(const uint16_t *)"\0\1" >> 8)

namespace aws {
namespace gg {

static constexpr int UINT64_MAX_DECIMAL_COUNT = 19;

static auto my_htonll(std::uint64_t h) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-cstyle-cast)
    if (IS_LITTLE_ENDIAN > 0) {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
        static_assert(CHAR_BIT == 8, "Char must be 8 bits");
        constexpr int shift_bytes1 = 8;
        constexpr int shift_bytes2 = 16;
        constexpr int shift_bytes4 = 32;
        h = ((h & UINT64_C(0x00FF00FF00FF00FF)) << shift_bytes1) | ((h & UINT64_C(0xFF00FF00FF00FF00)) >> shift_bytes1);
        h = ((h & UINT64_C(0x0000FFFF0000FFFF)) << shift_bytes2) | ((h & UINT64_C(0xFFFF0000FFFF0000)) >> shift_bytes2);
        h = ((h & UINT64_C(0x00000000FFFFFFFF)) << shift_bytes4) | ((h & UINT64_C(0xFFFFFFFF00000000)) >> shift_bytes4);
    }
    return h;
}

static auto my_ntohll(const std::uint64_t h) {
    return my_htonll(h);
}

static auto my_htonl(std::uint32_t h) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-cstyle-cast)
    if (IS_LITTLE_ENDIAN > 0) {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
        h = (((h & 0xFF000000U) >> 24) | ((h & 0x00FF0000U) >> 8) | ((h & 0x0000FF00U) << 8) |
             // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
             ((h & 0x000000FFU) << 24));
    }
    return h;
}
static auto my_ntohl(const std::uint32_t h) {
    return my_htonl(h);
}

constexpr uint8_t HEADER_SIZE = 32U;
constexpr int32_t MAGIC_BYTES = 0xAAAAAA;
constexpr uint8_t VERSION = 0x01U;
constexpr int32_t MAGIC_AND_VERSION = MAGIC_BYTES << 8 | static_cast<int8_t>(VERSION);

#pragma pack(push, 4)
struct LogEntryHeader {
    int32_t magic_and_version = static_cast<int32_t>(my_htonl(static_cast<std::uint32_t>(MAGIC_AND_VERSION)));
    int32_t relative_sequence_number = 0;
    int32_t byte_position = 0;
    int64_t crc = 0;
    int64_t timestamp = 0;
    int32_t payload_length_bytes = 0;
};
#pragma pack(pop)

static_assert(sizeof(LogEntryHeader) == HEADER_SIZE, "Header size must be 32 bytes!");

static std::string string(const StreamErrorCode e) noexcept {
    std::string v{};
    switch (e) {
    case StreamErrorCode::NoError:
        v = "NoError";
        break;
    case StreamErrorCode::RecordNotFound:
        v = "RecordNotFound";
        break;
    case StreamErrorCode::RecordDataCorrupted:
        v = "RecordDataCorrupted";
        break;
    case StreamErrorCode::HeaderDataCorrupted:
        v = "HeaderDataCorrupted";
        break;
    case StreamErrorCode::RecordTooLarge:
        v = "RecordTooLarge";
        break;
    case StreamErrorCode::ReadError:
        v = "ReadError";
        break;
    case StreamErrorCode::WriteError:
        v = "WriteError";
        break;
    case StreamErrorCode::StreamClosed:
        v = "StreamClosed";
        break;
    case StreamErrorCode::InvalidArguments:
        v = "InvalidArguments";
        break;
    case StreamErrorCode::Unknown:
        v = "Unknown";
        break;
    case StreamErrorCode::DiskFull:
        v = "DiskFull";
        break;
    case StreamErrorCode::IteratorNotFound:
        v = "IteratorNotFound";
        break;
    case StreamErrorCode::StreamFull:
        v = "StreamFull";
        break;
    }
    return v;
}

FileSegment::FileSegment(const uint64_t base, std::shared_ptr<FileSystemInterface> interface,
                         std::shared_ptr<logging::Logger> logger) noexcept
    : _file_implementation(std::move(interface)), _logger(std::move(logger)), _base_seq_num(base),
      _highest_seq_num(base) {
    std::ostringstream oss;
    oss << std::setw(UINT64_MAX_DECIMAL_COUNT) << std::setfill('0') << _base_seq_num << ".log";

    _segment_id = oss.str();
}

void FileSegment::truncateAndLog(const uint32_t truncate, const StreamError &err) const noexcept {
    if (_logger && (_logger->level <= logging::LogLevel::Warning)) {
        auto message = std::string{"Truncating "} + _segment_id + " to a length of " + std::to_string(truncate);
        if (!err.msg.empty()) {
            message += " because " + err.msg;
        } else {
            message += " because " + string(err.code);
        }
        _logger->log(logging::LogLevel::Warning, message);
    }
    std::ignore = _f->truncate(truncate);
}

StreamError FileSegment::open(const bool full_corruption_check_on_open) noexcept {
    auto file_or = _file_implementation->open(_segment_id);
    if (!file_or.ok()) {
        return StreamError{StreamErrorCode::WriteError, file_or.err().msg};
    }

    _f = std::move(file_or.val());
    uint32_t offset = 0U;

    while (true) {
        const auto header_data_or = _f->read(offset, offset + HEADER_SIZE);
        if (!header_data_or.ok()) {
            if (header_data_or.err().code == FileErrorCode::EndOfFile) {
                // If we reached the end of the file, there could have been extra data at the end, but less
                // than what we were hoping to read. Truncate the file now so that everything before this point
                // is known valid and everything after is gone.
                std::ignore = _f->truncate(offset);
                return StreamError{StreamErrorCode::NoError, {}};
            }

            truncateAndLog(offset, StreamError{StreamErrorCode::ReadError, header_data_or.err().msg});
            continue;
        }
        const LogEntryHeader header = convertSliceToHeader(header_data_or.val());

        if (header.magic_and_version != MAGIC_AND_VERSION) {
            truncateAndLog(offset, StreamError{StreamErrorCode::HeaderDataCorrupted, {}});
            continue;
        }
        if (full_corruption_check_on_open) {
            auto value_or = read(static_cast<std::uint64_t>(header.relative_sequence_number) + _base_seq_num,
                                 ReadOptions{true, false, offset});
            if (!value_or.ok()) {
                truncateAndLog(offset, value_or.err());
                continue;
            }
        }

        offset += HEADER_SIZE;
        offset += static_cast<uint32_t>(header.payload_length_bytes);
        _total_bytes += static_cast<std::uint32_t>(header.payload_length_bytes) + HEADER_SIZE;
        _highest_seq_num =
            std::max(_highest_seq_num, _base_seq_num + static_cast<std::uint64_t>(header.relative_sequence_number));
    }
}

LogEntryHeader FileSegment::convertSliceToHeader(const OwnedSlice &data) noexcept {
    LogEntryHeader header{};
    // coverity[autosar_cpp14_a12_0_2_violation] Use memcpy instead of reinterpret cast to avoid UB.
    std::ignore = memcpy(&header, data.data(), sizeof(LogEntryHeader));

    header.payload_length_bytes =
        static_cast<int32_t>(my_ntohl(static_cast<std::uint32_t>(header.payload_length_bytes)));
    header.relative_sequence_number =
        static_cast<int32_t>(my_ntohl(static_cast<std::uint32_t>(header.relative_sequence_number)));
    header.byte_position = static_cast<int32_t>(my_ntohl(static_cast<std::uint32_t>(header.byte_position)));
    header.crc = static_cast<int64_t>(my_ntohll(static_cast<std::uint64_t>(header.crc)));
    header.timestamp = static_cast<int64_t>(my_ntohll(static_cast<std::uint64_t>(header.timestamp)));
    header.magic_and_version = static_cast<int32_t>(my_ntohl(static_cast<std::uint32_t>(header.magic_and_version)));
    return header;
}

expected<uint64_t, FileError> FileSegment::append(const BorrowedSlice d, const int64_t timestamp_ms,
                                                  const uint64_t sequence_number, const bool sync) noexcept {
    const auto ts = static_cast<int64_t>(my_htonll(static_cast<std::uint64_t>(timestamp_ms)));
    const auto data_len_swap = static_cast<int32_t>(my_htonl(d.size()));
    const auto byte_position = static_cast<int32_t>(my_htonl(_total_bytes));

    const auto crc = static_cast<int64_t>(my_htonll(
        crc32::crc32_of(BorrowedSlice{&ts, sizeof(ts)}, BorrowedSlice{&data_len_swap, sizeof(data_len_swap)}, d)));
    const auto header = LogEntryHeader{
        static_cast<int32_t>(my_htonl(static_cast<std::uint32_t>(MAGIC_AND_VERSION))),
        static_cast<int32_t>(my_htonl(static_cast<std::uint32_t>(sequence_number - _base_seq_num))),
        byte_position,
        crc,
        ts,
        static_cast<int32_t>(my_htonl(d.size())),
    };

    // If an error happens when appending, truncate the file to the current size so that we don't have any
    // partial data in the file, and then return the error.
    auto e = _f->append(BorrowedSlice{&header, sizeof(header)});
    if (!e.ok()) {
        std::ignore = _f->truncate(_total_bytes);
        return e;
    }
    e = _f->append(d);
    if (!e.ok()) {
        std::ignore = _f->truncate(_total_bytes);
        return e;
    }
    e = _f->flush();
    if (!e.ok()) {
        std::ignore = _f->truncate(_total_bytes);
        return e;
    }

    if (sync) {
        _f->sync();
    }

    _highest_seq_num = std::max(_highest_seq_num, sequence_number);
    _total_bytes += d.size() + static_cast<uint32_t>(sizeof(LogEntryHeader));

    return d.size() + sizeof(LogEntryHeader);
}

expected<OwnedRecord, StreamError> FileSegment::read(const uint64_t sequence_number,
                                                     const ReadOptions &read_options) const noexcept {
    // We will try to find the record by reading the segment starting at the offset.
    // If a suggested starting position within the segment was suggested to us, we start from further into the file.
    // If any error occurs with the suggested starting point, we will restart from the beginning of the file.
    // Any failures during reading without a suggested started point are raised immediately.

    auto offset = read_options.suggested_start;
    auto suggested_start = offset != 0U;

    while (true) {
        auto header_data_or = _f->read(offset, offset + HEADER_SIZE);
        if (!header_data_or.ok()) {
            if (suggested_start) {
                offset = 0U;
                suggested_start = false;
                continue;
            }
            if (header_data_or.err().code == FileErrorCode::EndOfFile) {
                return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
            }
            return StreamError{StreamErrorCode::ReadError, header_data_or.err().msg};
        }
        const LogEntryHeader header = convertSliceToHeader(header_data_or.val());

        if (header.magic_and_version != MAGIC_AND_VERSION) {
            return StreamError{StreamErrorCode::HeaderDataCorrupted, {}};
        }

        const auto rel = sequence_number - _base_seq_num;
        const auto expected_rel_seq_num = static_cast<int32_t>(rel);

        // If the record we read is after the one we wanted, and we're not allowed to return later records, we must fail
        if ((header.relative_sequence_number > expected_rel_seq_num) && (!read_options.may_return_later_records)) {
            return StreamError{StreamErrorCode::RecordNotFound, RecordNotFoundErrorStr};
        }

        // We found the one we want, or the next available sequence number was acceptable to us
        if ((header.relative_sequence_number == expected_rel_seq_num) ||
            ((header.relative_sequence_number > expected_rel_seq_num) && read_options.may_return_later_records)) {
            auto data_or = _f->read(offset + HEADER_SIZE,
                                    offset + HEADER_SIZE + static_cast<std::uint32_t>(header.payload_length_bytes));
            if (!data_or.ok()) {
                return StreamError{StreamErrorCode::ReadError, header_data_or.err().msg};
            }
            auto data = std::move(data_or.val());
            const auto data_len_swap =
                static_cast<int32_t>(my_htonl(static_cast<std::uint32_t>(header.payload_length_bytes)));
            const auto ts_swap = static_cast<int64_t>(my_htonll(static_cast<std::uint64_t>(header.timestamp)));

            if (read_options.check_for_corruption &&
                (header.crc !=
                 static_cast<int64_t>(crc32::crc32_of(BorrowedSlice{&ts_swap, sizeof(ts_swap)},
                                                      BorrowedSlice{&data_len_swap, sizeof(data_len_swap)},
                                                      BorrowedSlice{data.data(), data.size()})))) {
                return StreamError{StreamErrorCode::RecordDataCorrupted, {}};
            }

            return OwnedRecord{
                std::move(data),
                header.timestamp,
                sequence_number,
                offset + HEADER_SIZE,
            };
        }

        offset += HEADER_SIZE;
        offset += static_cast<std::uint32_t>(header.payload_length_bytes);
    }
}

void FileSegment::remove() noexcept {
    // Close file handle, then delete file
    _f.reset();
    const auto e = _file_implementation->remove(_segment_id);
    if ((!e.ok()) && (_logger->level <= logging::LogLevel::Warning)) {
        _logger->log(logging::LogLevel::Warning, "Issue deleting " + _segment_id + " due to: " + e.msg);
    }
}
} // namespace gg
} // namespace aws
