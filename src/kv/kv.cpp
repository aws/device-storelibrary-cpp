#include <aws/store/common/crc32.hpp>
#include <aws/store/common/expected.hpp>
#include <aws/store/common/logging.hpp>
#include <aws/store/common/slices.hpp>
#include <aws/store/common/util.hpp>
#include <aws/store/filesystem/filesystem.hpp>
#include <aws/store/kv/kv.hpp>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace aws {
namespace store {
namespace kv {
constexpr static uint8_t DELETED_FLAG = 0x01U;

common::Expected<std::shared_ptr<KV>, KVError> KV::openOrCreate(KVOptions &&o) noexcept {
    auto opts = std::move(o);
    if (opts.identifier.empty()) {
        return KVError{KVErrorCodes::InvalidArguments, "Identifier cannot be empty"};
    }
    if (opts.filesystem_implementation == nullptr) {
        return KVError{KVErrorCodes::InvalidArguments, "Filesystem implementation cannot be null"};
    }

    // coverity[autosar_cpp14_a20_8_6_violation] constructor is private, cannot use make_shared
    auto kv = std::shared_ptr<KV>(new KV(std::move(opts)));
    auto err = kv->initialize();
    if (!err.ok()) {
        return err;
    }
    return kv;
}

static std::string string(const KVErrorCodes e) {
    std::string v{};
    switch (e) {
    case KVErrorCodes::NoError:
        v = "NoError";
        break;
    case KVErrorCodes::KeyNotFound:
        v = "KeyNotFound";
        break;
    case KVErrorCodes::ReadError:
        v = "ReadError";
        break;
    case KVErrorCodes::WriteError:
        v = "WriteError";
        break;
    case KVErrorCodes::HeaderCorrupted:
        v = "HeaderCorrupted";
        break;
    case KVErrorCodes::DataCorrupted:
        v = "DataCorrupted";
        break;
    case KVErrorCodes::EndOfFile:
        v = "EndOfFile";
        break;
    case KVErrorCodes::InvalidArguments:
        v = "InvalidArguments";
        break;
    case KVErrorCodes::Unknown:
        v = "Unknown";
        break;
    case KVErrorCodes::DiskFull:
        v = "Disk full";
        break;
    }

    return v;
}

void KV::truncateAndLog(const uint32_t truncate, const KVError &err) const noexcept {
    if (_opts.logger && (_opts.logger->level <= logging::LogLevel::Warning)) {
        auto message = std::string{"Truncating "} + _opts.identifier + " to a length of " + std::to_string(truncate);
        if (!err.msg.empty()) {
            message += " because " + err.msg;
        } else {
            message += " because " + string(err.code);
        }
        _opts.logger->log(logging::LogLevel::Warning, message);
    }
    std::ignore = _f->truncate(truncate);
}

KVError KV::openFile() noexcept {
    // Read from main file if it exists. If it doesn't exist, use the shadow file if available.
    if (_opts.filesystem_implementation->exists(_opts.identifier)) {
        _opts.filesystem_implementation->remove(_shadow_name);
    } else if (_opts.filesystem_implementation->exists(_shadow_name)) {
        _opts.filesystem_implementation->rename(_shadow_name, _opts.identifier);
    }

    auto e = _opts.filesystem_implementation->open(_opts.identifier);
    if (!e.ok()) {
        return KVError{KVErrorCodes::ReadError, e.err().msg};
    }
    _f = std::move(e.val());
    return KVError{KVErrorCodes::NoError, {}};
}

KVError KV::initialize() noexcept {
    std::lock_guard<std::mutex> lock(_lock);

    auto err = openFile();
    if (!err.ok()) {
        return err;
    }

    while (true) {
        auto beginning_pointer = _byte_position;
        auto header_or = readHeaderFrom(beginning_pointer);
        if (!header_or.ok()) {
            if (header_or.err().code == KVErrorCodes::EndOfFile) {
                // If we reached the end of the file, there could have been extra data at the end, but less
                // than what we were hoping to read. Truncate the file now so that everything before this point
                // is known valid and everything after is gone.
                std::ignore = _f->truncate(beginning_pointer);
                return KVError{KVErrorCodes::NoError, {}};
            }

            truncateAndLog(beginning_pointer, header_or.err());
            continue;
        }
        const auto header = header_or.val();

        // Checking key corruption is currently not supported. This can be added by computing crc32 on the key
        auto key_or = readKeyFrom(beginning_pointer, header.key_length);
        if (!key_or.ok()) {
            truncateAndLog(beginning_pointer, key_or.err());
            continue;
        }

        if (_opts.full_corruption_check_on_open) {
            auto value_or = readValueFrom(beginning_pointer, header);
            if (!value_or.ok()) {
                truncateAndLog(beginning_pointer, value_or.err());
                continue;
            }
        }

        const uint32_t added_size = smallSizeOf<detail::KVHeader>() + header.key_length + header.value_length;

        // Update our internal mapping to add, remove, or update the key's pointer as necessary
        addOrRemoveKeyInInitialization(key_or.val(), beginning_pointer, added_size, header.flags);

        _byte_position += added_size;
    }

    return KVError{KVErrorCodes::NoError, {}};
}

// Only use this method during KV::initialize.
inline void KV::addOrRemoveKeyInInitialization(const std::string &key, const uint32_t beginning_pointer,
                                               const uint32_t added_size, const uint8_t flags) noexcept {
    const bool isDeleted = static_cast<int8_t>(flags) & static_cast<int8_t>(DELETED_FLAG);
    if (isDeleted) {
        std::ignore = removeKey(key);
        // Count deleted entry as added because compaction would be helpful in shrinking the map.
        _added_bytes += added_size;
    } else {
        bool found = false;
        for (auto &point : _key_pointers) {
            if (point.first == key) {
                point.second = beginning_pointer;
                found = true;
                // Since we already had this key in our map, the one we just read should be considered as "added",
                // meaning that compaction would be helpful in shrinking the map.
                _added_bytes += added_size;
                break;
            }
        }

        if (!found) {
            _key_pointers.emplace_back(key, beginning_pointer);
        }
    }
}

static KVError fileErrorToKVError(const filesystem::FileError &e) {
    auto v = KVError{KVErrorCodes::NoError, e.msg};
    switch (e.code) {
    case filesystem::FileErrorCode::NoError:
        v = KVError{KVErrorCodes::NoError, e.msg};
        break;
    case filesystem::FileErrorCode::InvalidArguments:
        v = KVError{KVErrorCodes::InvalidArguments, e.msg};
        break;
    case filesystem::FileErrorCode::EndOfFile:
        v = KVError{KVErrorCodes::EndOfFile, e.msg};
        break;
    case filesystem::FileErrorCode::AccessDenied:
        v = KVError{KVErrorCodes::WriteError, e.msg};
        break;
    case filesystem::FileErrorCode::TooManyOpenFiles:
        v = KVError{KVErrorCodes::WriteError, e.msg};
        break;
    case filesystem::FileErrorCode::DiskFull:
        v = KVError{KVErrorCodes::DiskFull, e.msg};
        break;
    case filesystem::FileErrorCode::FileDoesNotExist:
        v = KVError{KVErrorCodes::ReadError, e.msg};
        break;
    case filesystem::FileErrorCode::IOError:
        v = KVError{KVErrorCodes::ReadError, e.msg};
        break;
    case filesystem::FileErrorCode::Unknown:
        v = KVError{KVErrorCodes::ReadError, e.msg};
        break;
    }

    return v;
}

common::Expected<detail::KVHeader, KVError> KV::readHeaderFrom(const uint32_t begin) const noexcept {
    auto header_or = _f->read(begin, begin + smallSizeOf<detail::KVHeader>());
    if (!header_or.ok()) {
        return fileErrorToKVError(header_or.err());
    }

    detail::KVHeader ret{};
    // coverity[autosar_cpp14_a12_0_2_violation] Use memcpy instead of reinterpret cast to avoid UB
    std::ignore = memcpy(&ret, header_or.val().data(), sizeof(detail::KVHeader));

    if (static_cast<int8_t>(ret.magic_and_version) != static_cast<int8_t>(detail::MAGIC_AND_VERSION)) {
        return KVError{KVErrorCodes::HeaderCorrupted, "Invalid magic and version"};
    }

    return ret;
}

common::Expected<std::string, KVError> KV::readKeyFrom(const uint32_t begin,
                                                       const detail::key_length_type key_length) const noexcept {
    auto key_or =
        _f->read(begin + smallSizeOf<detail::KVHeader>(), begin + smallSizeOf<detail::KVHeader>() + key_length);
    if (!key_or.ok()) {
        return fileErrorToKVError(key_or.err());
    }
    return key_or.val().string();
}

common::Expected<common::OwnedSlice, KVError> KV::readValueFrom(const uint32_t begin) const noexcept {
    auto header_or = readHeaderFrom(begin);
    if (!header_or.ok()) {
        return header_or.err();
    }
    return readValueFrom(begin, header_or.val());
}

common::Expected<common::OwnedSlice, KVError> KV::readValueFrom(const uint32_t begin,
                                                                const detail::KVHeader &header) const noexcept {
    auto value_or = _f->read(begin + smallSizeOf<detail::KVHeader>() + header.key_length,
                             begin + smallSizeOf<detail::KVHeader>() + header.key_length + header.value_length);
    if (!value_or.ok()) {
        return fileErrorToKVError(value_or.err());
    }
    const auto crc = common::crc32::crc32_of({common::BorrowedSlice{&header.flags, sizeof(header.flags)},
                                              common::BorrowedSlice{&header.key_length, sizeof(header.key_length)},
                                              common::BorrowedSlice{&header.value_length, sizeof(header.value_length)},
                                              common::BorrowedSlice{value_or.val().data(), value_or.val().size()}});

    if (crc != header.crc32) {
        return KVError{KVErrorCodes::DataCorrupted, "CRC mismatch"};
    }
    return common::Expected<common::OwnedSlice, KVError>{std::move(value_or.val())};
}

common::Expected<std::vector<std::string>, KVError> KV::listKeys() const noexcept {
    std::lock_guard<std::mutex> lock(_lock);

    std::vector<std::string> keys{};
    keys.reserve(_key_pointers.size());
    for (const auto &point : _key_pointers) {
        keys.emplace_back(point.first);
    }
    return keys;
}

KVError KV::compact() noexcept {
    std::lock_guard<std::mutex> lock(_lock);
    return compactNoLock();
}

std::uint32_t KV::currentSizeBytes() const noexcept {
    return _byte_position;
}

common::Expected<common::OwnedSlice, KVError> KV::get(const std::string &key) const noexcept {
    std::lock_guard<std::mutex> lock(_lock);

    for (const auto &point : _key_pointers) {
        if (point.first == key) {
            return readValueFrom(point.second);
        }
    }
    return KVError{KVErrorCodes::KeyNotFound, {}};
}

filesystem::FileError KV::appendMultiple(const std::initializer_list<common::BorrowedSlice> args) const noexcept {
    // Try to append any non-zero data, rolling back all appends if any fails by truncating the file.
    for (auto arg : args) {
        if (arg.size() > 0) {
            auto e = _f->append(arg);
            if (!e.ok()) {
                std::ignore = _f->truncate(_byte_position);
                return e;
            }
        }
    }
    auto e = _f->flush();
    if (!e.ok()) {
        std::ignore = _f->truncate(_byte_position);
        return e;
    }
    return filesystem::FileError{filesystem::FileErrorCode::NoError, {}};
}

inline KVError KV::writeEntry(const std::string &key, const common::BorrowedSlice data,
                              const uint8_t flags) const noexcept {
    const auto key_len = static_cast<detail::key_length_type>(key.length());
    const auto value_len = data.size();

    const auto crc = store::common::crc32::crc32_of(
        {common::BorrowedSlice{&flags, sizeof(flags)}, common::BorrowedSlice{&key_len, sizeof(key_len)},
         common::BorrowedSlice{&value_len, sizeof(value_len)}, common::BorrowedSlice{data.data(), data.size()}});

    const auto header = detail::KVHeader{
        detail::MAGIC_AND_VERSION, flags, key_len, crc, value_len,
    };

    return fileErrorToKVError(
        appendMultiple({common::BorrowedSlice(&header, sizeof(header)), common::BorrowedSlice{key}, data}));
}

KVError KV::put(const std::string &key, const common::BorrowedSlice data) noexcept {
    if (key.empty()) {
        return KVError{KVErrorCodes::InvalidArguments, "Key cannot be empty"};
    }
    if (key.length() >= static_cast<size_t>(detail::KEY_LENGTH_MAX)) {
        return KVError{KVErrorCodes::InvalidArguments,
                       "Key length cannot exceed " + std::to_string(static_cast<int32_t>(detail::KEY_LENGTH_MAX))};
    }
    if (data.size() >= detail::VALUE_LENGTH_MAX) {
        return KVError{KVErrorCodes::InvalidArguments,
                       "Value length cannot exceed " + std::to_string(detail::VALUE_LENGTH_MAX)};
    }

    std::lock_guard<std::mutex> lock(_lock);
    auto e = writeEntry(key, data, 0U);
    if (!e.ok()) {
        return e;
    }

    bool found = false;
    for (auto &point : _key_pointers) {
        if (point.first == key) {
            point.second = _byte_position;
            found = true;
            break;
        }
    }

    const uint32_t added_size = smallSizeOf<detail::KVHeader>() + static_cast<uint32_t>(key.length()) + data.size();
    if (found) {
        // If the key already existed in the map, then count the duplicated bytes to know when we need to compact.
        // Newly added keys do not count against compaction.
        _added_bytes += added_size;
    } else {
        _key_pointers.emplace_back(key, _byte_position);
    }

    _byte_position += added_size;

    return maybeCompact();
}

KVError KV::maybeCompact() noexcept {
    if ((_opts.compact_after > 0) && (static_cast<int64_t>(_added_bytes) > _opts.compact_after)) {
        // We're already holding the mutex, so call compactNoLock() directly.
        return compactNoLock();
    }

    return KVError{KVErrorCodes::NoError, {}};
}

common::Expected<uint32_t, KVError> KV::readWrite(const uint32_t begin, std::pair<std::string, uint32_t> &p,
                                                  filesystem::FileLike &f) noexcept {
    auto header_or = readHeaderFrom(p.second);
    if (!header_or.ok()) {
        return header_or.err();
    }
    const auto header = header_or.val();
    auto value_or = readValueFrom(p.second, header);
    if (!value_or.ok()) {
        return value_or.err();
    }
    const auto value = std::move(value_or.val());

    auto e = f.append(common::BorrowedSlice{&header, sizeof(header)});
    if (!e.ok()) {
        return fileErrorToKVError(e);
    }
    e = f.append(common::BorrowedSlice{p.first});
    if (!e.ok()) {
        return fileErrorToKVError(e);
    }
    e = f.append(common::BorrowedSlice{value.data(), value.size()});
    if (!e.ok()) {
        return fileErrorToKVError(e);
    }

    p.second = begin;
    return smallSizeOf<detail::KVHeader>() + header.key_length + header.value_length;
}

KV::KV(KVOptions &&opts) noexcept : _opts(std::move(opts)), _shadow_name(_opts.identifier + "s") {
}

KVError KV::compactNoLock() noexcept {
    // Remove any previous partially written shadow
    _opts.filesystem_implementation->remove(_shadow_name);
    auto shadow_or = _opts.filesystem_implementation->open(_shadow_name);
    if (!shadow_or.ok()) {
        return KVError{KVErrorCodes::WriteError, shadow_or.err().msg};
    }

    uint32_t new_byte_pos = 0U;
    std::vector<decltype(_key_pointers)::value_type> new_points{};
    new_points.reserve(_key_pointers.size());
    {
        auto shadow = std::move(shadow_or.val());
        for (const auto &in_point : _key_pointers) {
            auto point = in_point;
            auto err_or = readWrite(new_byte_pos, point, *shadow);
            if (!err_or.ok()) {
                // if key is corrupted, remove it from the map
                if ((err_or.err().code == KVErrorCodes::HeaderCorrupted) ||
                    (err_or.err().code == KVErrorCodes::DataCorrupted)) {
                    _opts.logger->log(logging::LogLevel::Warning, "Encountered corruption during compaction. Key <" +
                                                                      point.first + "> will be dropped.");
                    continue;
                }
                // Close and delete the partially written shadow
                shadow.reset();
                _opts.filesystem_implementation->remove(_shadow_name);
                return KVError{KVErrorCodes::WriteError, err_or.err().msg};
            }
            new_byte_pos += err_or.val();
            new_points.emplace_back(point);
        }

        auto e = shadow->flush();
        if (!e.ok()) {
            // Close and delete the partially written shadow
            shadow.reset();
            _opts.filesystem_implementation->remove(_shadow_name);
            return KVError{KVErrorCodes::WriteError, e.msg};
        }
        shadow->sync();
    }

    // Close our file handle before doing renames
    _f.reset(nullptr);
    // Overwrite the main file with the shadow we just wrote
    _opts.filesystem_implementation->rename(_shadow_name, _opts.identifier);
    // Open up the main file (which is the shadow that we just wrote)
    auto main_or = _opts.filesystem_implementation->open(_opts.identifier);
    if (!main_or.ok()) {
        return KVError{KVErrorCodes::ReadError, main_or.err().msg};
    }
    // Replace our internal filehandle to use the new main file
    _f = std::move(main_or.val());

    _added_bytes = 0U;
    _byte_position = new_byte_pos;
    _key_pointers = new_points;

    return KVError{KVErrorCodes::NoError, {}};
}

bool KV::removeKey(const std::string &key) noexcept {
    for (size_t i = 0U; i < _key_pointers.size(); i++) {
        if (_key_pointers[i].first == key) {
            auto it = _key_pointers.cbegin();
            std::advance(it, static_cast<int32_t>(i));
            std::ignore = _key_pointers.erase(it);
            return true;
        }
    }
    return false;
}

KVError KV::remove(const std::string &key) noexcept {
    std::lock_guard<std::mutex> lock(_lock);

    if (!removeKey(key)) {
        return KVError{KVErrorCodes::KeyNotFound, {}};
    }

    auto e = writeEntry(key, common::BorrowedSlice{}, DELETED_FLAG);
    if (!e.ok()) {
        return e;
    }

    const uint32_t added_size = smallSizeOf<detail::KVHeader>() + static_cast<detail::key_length_type>(key.length());
    _byte_position += added_size;
    _added_bytes += added_size;

    return maybeCompact();
}
} // namespace kv
} // namespace store
} // namespace aws
