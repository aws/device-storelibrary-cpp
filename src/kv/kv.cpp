#include "kv/kv.hpp"
#include "common/crc32.hpp"

namespace aws {
namespace gg {
namespace kv {
using namespace detail;
constexpr static uint8_t DELETED_FLAG = 0x01;

expected<std::shared_ptr<KV>, KVError> KV::openOrCreate(KVOptions &&opts) {
    if (opts.identifier.empty()) {
        return KVError{KVErrorCodes::InvalidArguments, "Identifier cannot be empty"};
    }
    if (opts.filesystem_implementation == nullptr) {
        return KVError{KVErrorCodes::InvalidArguments, "Filesystem implementation cannot be null"};
    }

    auto kv = std::shared_ptr<KV>(new KV(std::move(opts)));
    auto ok = kv->initialize();
    if (!ok) {
        return ok;
    }
    return kv;
}

static std::string string(const KVErrorCodes e) {
    using namespace std::string_literals;
    switch (e) {
    case KVErrorCodes::NoError:
        return "NoError"s;
    case KVErrorCodes::KeyNotFound:
        return "KeyNotFound"s;
    case KVErrorCodes::ReadError:
        return "ReadError"s;
    case KVErrorCodes::WriteError:
        return "WriteError"s;
    case KVErrorCodes::HeaderCorrupted:
        return "HeaderCorrupted"s;
    case KVErrorCodes::DataCorrupted:
        return "DataCorrupted"s;
    case KVErrorCodes::EndOfFile:
        return "EndOfFile"s;
    case KVErrorCodes::InvalidArguments:
        return "InvalidArguments"s;
    case KVErrorCodes::Unknown:
        return "Unknown"s;
    case KVErrorCodes::DiskFull:
        return "Disk full"s;
    }
    // Unreachable.
    return {};
}

void KV::truncateAndLog(uint32_t truncate, const KVError &err) const noexcept {
    if (_opts.logger && _opts.logger->level <= logging::LogLevel::Warning) {
        using namespace std::string_literals;
        auto message = "Truncating "s + _opts.identifier + " to a length of "s + std::to_string(truncate);
        if (!err.msg.empty()) {
            message += " because "s + err.msg;
        } else {
            message += " because "s + string(err.code);
        }
        _opts.logger->log(logging::LogLevel::Warning, message);
    }
    _f->truncate(truncate);
}

KVError KV::openFile() {
    // Read from main file if it exists. If it doesn't exist, use the shadow file if available.
    if (_opts.filesystem_implementation->exists(_opts.identifier)) {
        _opts.filesystem_implementation->remove(_shadow_name);
    } else if (_opts.filesystem_implementation->exists(_shadow_name)) {
        _opts.filesystem_implementation->rename(_shadow_name, _opts.identifier);
    }

    auto e = _opts.filesystem_implementation->open(_opts.identifier);
    if (!e) {
        return KVError{KVErrorCodes::ReadError, e.err().msg};
    }
    _f = std::move(e.val());
    return KVError{KVErrorCodes::NoError, {}};
}

KVError KV::initialize() {
    std::lock_guard<std::mutex> lock(_lock);

    auto ok = openFile();
    if (!ok) {
        return ok;
    }

    while (true) {
        uint32_t beginning_pointer = _byte_position;
        auto header_or = readHeaderFrom(beginning_pointer);
        if (!header_or) {
            if (header_or.err().code == KVErrorCodes::EndOfFile) {
                // If we reached the end of the file, there could have been extra data at the end, but less
                // than what we were hoping to read. Truncate the file now so that everything before this point
                // is known valid and everything after is gone.
                _f->truncate(beginning_pointer);
                return KVError{KVErrorCodes::NoError, {}};
            }

            truncateAndLog(beginning_pointer, header_or.err());
            continue;
        }
        const auto header = header_or.val();

        auto key_or = readKeyFrom(beginning_pointer, header.key_length);
        if (!key_or) {
            truncateAndLog(beginning_pointer, key_or.err());
            continue;
        }

        if (_opts.full_corruption_check_on_open) {
            auto value_or = readValueFrom(beginning_pointer, header.key_length, header.value_length);

            if (!value_or) {
                truncateAndLog(beginning_pointer, value_or.err());
                continue;
            }

            auto crc = crc32::crc32_of(BorrowedSlice{&header.flags, sizeof(header.flags)},
                                       BorrowedSlice{&header.key_length, sizeof(header.key_length)},
                                       BorrowedSlice{&header.value_length, sizeof(header.value_length)},
                                       BorrowedSlice{value_or.val().data(), value_or.val().size()});
            if (crc != header.crc32) {
                truncateAndLog(beginning_pointer, KVError{KVErrorCodes::DataCorrupted, {}});
                continue;
            }
        }

        const uint32_t added_size = sizeof(header) + header.key_length + header.value_length;

        // Update our internal mapping to add, remove, or update the key's pointer as necessary
        addOrRemoveKeyInInitialization(key_or.val(), beginning_pointer, added_size, header.flags);

        _byte_position += added_size;
    }

    return KVError{KVErrorCodes::NoError, {}};
}

// Only use this method during KV::initialize.
void inline KV::addOrRemoveKeyInInitialization(const std::string &key, const uint32_t beginning_pointer,
                                               const uint32_t added_size, uint8_t flags) {
    if (flags & DELETED_FLAG) {
        removeKey(key);
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

static KVError fileErrorToKVError(const FileError &e) {
    switch (e.code) {
    case FileErrorCode::NoError:
        return KVError{KVErrorCodes::NoError, e.msg};
    case FileErrorCode::InvalidArguments:
        return KVError{KVErrorCodes::InvalidArguments, e.msg};
    case FileErrorCode::EndOfFile:
        return KVError{KVErrorCodes::EndOfFile, e.msg};
    case FileErrorCode::AccessDenied:
        [[fallthrough]];
    case FileErrorCode::TooManyOpenFiles:
        return KVError{KVErrorCodes::WriteError, e.msg};
    case FileErrorCode::DiskFull:
        return KVError{KVErrorCodes::DiskFull, e.msg};
    case FileErrorCode::FileDoesNotExist:
        [[fallthrough]];
    case FileErrorCode::IOError:
        [[fallthrough]];
    case FileErrorCode::Unknown:
        return KVError{KVErrorCodes::ReadError, e.msg};
    }
    // Unreachable.
    return {};
}

expected<KVHeader, KVError> KV::readHeaderFrom(uint32_t begin) const {
    auto header_or = _f->read(begin, begin + sizeof(KVHeader));
    if (!header_or) {
        return fileErrorToKVError(header_or.err());
    }

    KVHeader ret{};
    // Use memcpy instead of reinterpret cast to avoid UB.
    memcpy(&ret, header_or.val().data(), sizeof(KVHeader));

    if (ret.magic_and_version != MAGIC_AND_VERSION) {
        return KVError{KVErrorCodes::HeaderCorrupted, "Invalid magic and version"};
    }

    return ret;
}

expected<std::string, KVError> KV::readKeyFrom(uint32_t begin, key_length_type key_length) const {
    auto key_or = _f->read(begin + sizeof(KVHeader), begin + sizeof(KVHeader) + key_length);
    if (!key_or) {
        return fileErrorToKVError(key_or.err());
    }
    return key_or.val().string();
}

expected<OwnedSlice, KVError> KV::readValueFrom(const uint32_t begin) const {
    auto header_or = readHeaderFrom(begin);
    if (!header_or) {
        return header_or.err();
    }
    auto header = header_or.val();

    auto value_or = readValueFrom(begin, header.key_length, header.value_length);
    if (!value_or) {
        return value_or;
    }
    auto crc = crc32::crc32_of(BorrowedSlice{&header.flags, sizeof(header.flags)},
                               BorrowedSlice{&header.key_length, sizeof(header.key_length)},
                               BorrowedSlice{&header.value_length, sizeof(header.value_length)},
                               BorrowedSlice{value_or.val().data(), value_or.val().size()});

    if (crc != header.crc32) {
        return KVError{KVErrorCodes::DataCorrupted, "CRC mismatch"};
    }
    return value_or;
}

expected<OwnedSlice, KVError> KV::readValueFrom(const uint32_t begin, key_length_type key_length,
                                                value_length_type value_length) const {
    auto value_or =
        _f->read(begin + sizeof(KVHeader) + key_length, begin + sizeof(KVHeader) + key_length + value_length);
    if (!value_or) {
        return fileErrorToKVError(value_or.err());
    }
    return std::move(value_or.val());
}

expected<std::vector<std::string>, KVError> KV::listKeys() const {
    std::lock_guard<std::mutex> lock(_lock);

    std::vector<std::string> keys{};
    keys.reserve(_key_pointers.size());
    for (const auto &point : _key_pointers) {
        keys.emplace_back(point.first);
    }
    return keys;
}

expected<OwnedSlice, KVError> KV::get(const std::string &key) const {
    std::lock_guard<std::mutex> lock(_lock);

    for (const auto &point : _key_pointers) {
        if (point.first == key) {
            return readValueFrom(point.second);
        }
    }
    return KVError{KVErrorCodes::KeyNotFound, {}};
}

template <typename... Args> inline FileError KV::appendMultiple(const Args &...args) const noexcept {
    // Try to append any non-zero data, rolling back all appends if any fails by truncating the file.
    for (auto arg : {args...}) {
        if (arg.size() > 0) {
            auto e = _f->append(arg);
            if (!e) {
                _f->truncate(_byte_position);
                return e;
            }
        }
    }
    auto e = _f->flush();
    if (!e) {
        _f->truncate(_byte_position);
        return e;
    }
    return FileError{FileErrorCode::NoError, {}};
}

inline KVError KV::writeEntry(const std::string &key, const BorrowedSlice data, const uint8_t flags) const noexcept {
    auto key_len = static_cast<key_length_type>(key.length());
    auto value_len = data.size();

    auto crc = crc32::crc32_of(BorrowedSlice{&flags, sizeof(flags)}, BorrowedSlice{&key_len, sizeof(key_len)},
                               BorrowedSlice{&value_len, sizeof(value_len)}, BorrowedSlice{data.data(), data.size()});

    auto header = KVHeader{
        .magic_and_version = MAGIC_AND_VERSION,
        .flags = flags,
        .key_length = key_len,
        .crc32 = crc,
        .value_length = value_len,
    };

    return fileErrorToKVError(appendMultiple(BorrowedSlice(&header, sizeof(header)), BorrowedSlice{key}, data));
}

KVError KV::put(const std::string &key, BorrowedSlice data) {
    if (key.empty()) {
        return KVError{KVErrorCodes::InvalidArguments, "Key cannot be empty"};
    }
    if (key.length() >= KEY_LENGTH_MAX) {
        return KVError{KVErrorCodes::InvalidArguments, "Key length cannot exceed " + std::to_string(KEY_LENGTH_MAX)};
    }
    if (data.size() >= VALUE_LENGTH_MAX) {
        return KVError{KVErrorCodes::InvalidArguments,
                       "Value length cannot exceed " + std::to_string(VALUE_LENGTH_MAX)};
    }

    std::lock_guard<std::mutex> lock(_lock);
    auto e = writeEntry(key, data, 0);
    if (!e) {
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

    const uint32_t added_size = sizeof(KVHeader) + static_cast<uint32_t>(key.length()) + data.size();
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
    if (_opts.compact_after > 0 && _added_bytes > static_cast<int64_t>(_opts.compact_after)) {
        // We're already holding the mutex, so call compactNoLock() directly.
        return compactNoLock();
    }

    return KVError{KVErrorCodes::NoError, {}};
}

expected<uint32_t, KVError> KV::readWrite(uint32_t begin, std::pair<std::string, uint32_t> &p, FileLike &f) {
    auto header_or = readHeaderFrom(p.second);
    if (!header_or) {
        return header_or.err();
    }
    auto header = header_or.val();
    auto value_or = readValueFrom(p.second, header.key_length, header.value_length);
    if (!value_or) {
        return value_or.err();
    }
    auto value = std::move(value_or.val());

    auto e = f.append(BorrowedSlice{&header, sizeof(header)});
    if (!e) {
        return fileErrorToKVError(e);
    }
    e = f.append(BorrowedSlice{p.first});
    if (!e) {
        return fileErrorToKVError(e);
    }
    e = f.append(BorrowedSlice{value.data(), value.size()});
    if (!e) {
        return fileErrorToKVError(e);
    }

    p.second = begin;
    return sizeof(header) + header.key_length + header.value_length;
}

KVError KV::compactNoLock() {
    // Remove any previous partially written shadow
    _opts.filesystem_implementation->remove(_shadow_name);
    auto shadow_or = _opts.filesystem_implementation->open(_shadow_name);
    if (!shadow_or) {
        return KVError{KVErrorCodes::WriteError, shadow_or.err().msg};
    }

    uint32_t new_byte_pos = 0;
    std::vector<decltype(_key_pointers)::value_type> new_points{};
    new_points.reserve(_key_pointers.size());
    {
        auto shadow = std::move(shadow_or.val());
        for (const auto &in_point : _key_pointers) {
            auto point = in_point;
            auto err_or = readWrite(new_byte_pos, point, *shadow);
            if (!err_or) {
                // Close and delete the partially written shadow
                shadow.reset();
                _opts.filesystem_implementation->remove(_shadow_name);
                return KVError{KVErrorCodes::WriteError, err_or.err().msg};
            }
            new_byte_pos += err_or.val();
            new_points.emplace_back(point);
        }

        auto e = shadow->flush();
        if (!e) {
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
    if (!main_or) {
        return KVError{KVErrorCodes::ReadError, main_or.err().msg};
    }
    // Replace our internal filehandle to use the new main file
    _f = std::move(main_or.val());

    _added_bytes = 0;
    _byte_position = new_byte_pos;
    _key_pointers = new_points;

    return KVError{KVErrorCodes::NoError, {}};
}

bool KV::removeKey(const std::string &key) {
    for (size_t i = 0; i < _key_pointers.size(); i++) {
        if (_key_pointers[i].first == key) {
            auto it = _key_pointers.begin();
            std::advance(it, i);
            _key_pointers.erase(it);
            return true;
        }
    }
    return false;
}

KVError KV::remove(const std::string &key) {
    std::lock_guard<std::mutex> lock(_lock);

    if (!removeKey(key)) {
        return KVError{KVErrorCodes::KeyNotFound, {}};
    }

    auto e = writeEntry(key, BorrowedSlice{}, DELETED_FLAG);
    if (!e) {
        return e;
    }

    auto added_size = sizeof(KVHeader) + key.length();
    _byte_position += added_size;
    _added_bytes += added_size;

    return maybeCompact();
}
} // namespace kv
} // namespace gg
} // namespace aws