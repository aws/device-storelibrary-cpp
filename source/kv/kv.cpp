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
    auto err = kv->initialize();
    if (err.code != KVErrorCodes::NoError) {
        return err;
    }
    return kv;
}

KVError KV::initialize() {
    std::lock_guard<std::mutex> lock(_lock);

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

    while (true) {
        uint32_t beginning_pointer = _byte_position;
        auto header_or = readHeaderFrom(beginning_pointer);
        if (!header_or) {
            if (header_or.err().code == KVErrorCodes::EndOfFile) {
                return KVError{KVErrorCodes::NoError, {}};
            }
            // TODO: More error handling, logging, etc. We're potentially throwing away data here
            _f->truncate(_byte_position);
            return header_or.err();
        }
        _byte_position += sizeof(KVHeader);

        if (header_or.val().key_length == 0) {
            continue;
        }

        auto key_or = readKeyFrom(beginning_pointer, header_or.val().key_length);
        if (!key_or) {
            return KVError{KVErrorCodes::ReadError, "Incomplete key read. " + key_or.err().msg};
        }
        _byte_position += header_or.val().key_length;

        if (header_or.val().flags & DELETED_FLAG) {
            [[maybe_unused]] auto _ = removeKey(key_or.val());
            continue;
        }

        _byte_position += header_or.val().value_length;

        bool found = false;
        for (auto &point : _key_pointers) {
            if (point.first == key_or.val()) {
                point.second = beginning_pointer;
                found = true;
                break;
            }
        }

        if (!found) {
            _key_pointers.emplace_back(key_or.val(), beginning_pointer);
        }
    }

    return KVError{KVErrorCodes::NoError, {}};
}

static KVError fileErrorToKVError(const FileError &e) {
    if (e.code == FileErrorCode::EndOfFile) {
        return KVError{KVErrorCodes::EndOfFile, e.msg};
    }
    return KVError{KVErrorCodes::ReadError, e.msg};
}

expected<KVHeader, KVError> KV::readHeaderFrom(uint32_t begin) const {
    auto header_or = _f->read(begin, begin + sizeof(KVHeader));
    if (!header_or) {
        return fileErrorToKVError(header_or.err());
    }
    auto const *header = reinterpret_cast<KVHeader *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        header_or.val().data());
    auto ret = KVHeader{*header};

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
        return std::move(header_or.err());
    }
    auto header = header_or.val();

    return readValueFrom(begin, header.key_length, header.value_length);
}

expected<OwnedSlice, KVError> KV::readValueFrom(const uint32_t begin, key_length_type key_length,
                                                value_length_type value_length) const {
    auto value_or =
        _f->read(begin + sizeof(KVHeader) + key_length, begin + sizeof(KVHeader) + key_length + value_length);
    if (!value_or) {
        return fileErrorToKVError(value_or.err());
    }
    // TODO: Check CRC
    return std::move(value_or.val());
}

expected<std::vector<std::string>, KVError> KV::listKeys() const {
    std::lock_guard<std::mutex> lock(_lock);

    std::vector<std::string> keys{_key_pointers.size()};
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

KVError KV::put(const std::string &key, BorrowedSlice data) {
    if (key.empty()) {
        return KVError{KVErrorCodes::InvalidArguments, "Key cannot be empty"};
    }
    if (key.length() > KEY_LENGTH_MAX) {
        return KVError{KVErrorCodes::InvalidArguments, "Key length cannot exceed " + std::to_string(KEY_LENGTH_MAX)};
    }
    if (data.size() > VALUE_LENGTH_MAX) {
        return KVError{KVErrorCodes::InvalidArguments,
                       "Value length cannot exceed " + std::to_string(VALUE_LENGTH_MAX)};
    }

    std::lock_guard<std::mutex> lock(_lock);
    auto key_len = static_cast<key_length_type>(key.length());
    auto value_len = static_cast<value_length_type>(data.size());
    uint8_t flags = 0;
    auto header = KVHeader{
        .magic_and_version = MAGIC_AND_VERSION,
        .flags = flags,
        .key_length = key_len,
        .crc32 = crc32::update(
            crc32::update(crc32::update(crc32::update(0, &flags, sizeof(flags)), &key_len, sizeof(key_len)), &value_len,
                          sizeof(value_len)),
            data.data(), data.size()),
        .value_length = value_len,
    };
    _f->append(
        BorrowedSlice(reinterpret_cast<const uint8_t *>(&header), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(header)));
    _f->append(BorrowedSlice{key});
    _f->append(data);

    bool found = false;
    for (auto &point : _key_pointers) {
        if (point.first == key) {
            point.second = _byte_position;
            found = true;
            break;
        }
    }

    if (found) {
        // If the key already existed in the map, then count the duplicated bytes to know when we need to compact.
        // Newly added keys do not count against compaction.
        _added_bytes += sizeof(header) + key_len + value_len;
    } else {
        _key_pointers.emplace_back(key, _byte_position);
    }

    _byte_position += sizeof(header) + key_len + value_len;

    if (_added_bytes > _opts.compact_after) {
        // We're already holding the mutex, so call compactNoLock() directly.
        return compactNoLock();
    }

    return KVError{KVErrorCodes::NoError, {}};
}

KVError KV::readWrite(std::pair<std::string, uint32_t> &p, FileLike &f) {
    auto header_or = readHeaderFrom(p.second);
    if (!header_or) {
        return std::move(header_or.err());
    }
    auto header = header_or.val();
    auto value_or = readValueFrom(p.second, header.key_length, header.value_length);
    if (!value_or) {
        return std::move(value_or.err());
    }
    auto value = std::move(value_or.val());

    f.append(
        BorrowedSlice{reinterpret_cast<const uint8_t *>(&header), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(header)});
    f.append(BorrowedSlice{p.first});
    f.append(BorrowedSlice{value.data(), value.size()});

    p.second = _byte_position;
    _byte_position += sizeof(header) + header.key_length + header.value_length;

    return KVError{KVErrorCodes::NoError, {}};
}

KVError KV::compactNoLock() {
    _f->flush();

    // Remove any previous partially written shadow
    _opts.filesystem_implementation->remove(_shadow_name);
    auto shadow_or = _opts.filesystem_implementation->open(_shadow_name);
    if (!shadow_or) {
        return KVError{KVErrorCodes::WriteError, shadow_or.err().msg};
    }

    // Reset internal counters
    _byte_position = 0;
    _added_bytes = 0;

    {
        auto shadow = std::move(shadow_or.val());

        for (auto &point : _key_pointers) {
            auto err = readWrite(point, *shadow);
            if (err.code != KVErrorCodes::NoError) {
                return KVError{KVErrorCodes::WriteError, err.msg};
            }
        }

        shadow->flush();
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

    key_length_type key_len = key.size();
    value_length_type value_len = 0;
    uint8_t flags = DELETED_FLAG;
    auto header = KVHeader{
        .magic_and_version = MAGIC_AND_VERSION,
        .flags = flags,
        .key_length = key_len,
        .crc32 = crc32::update(crc32::update(crc32::update(0, &flags, sizeof(flags)), &key_len, sizeof(key_len)),
                               &value_len, sizeof(value_len)),
        .value_length = value_len,
    };
    _f->append(
        BorrowedSlice(reinterpret_cast<const uint8_t *>(&header), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(header)));
    _f->append(BorrowedSlice{key});

    _byte_position += sizeof(header) + key_len;

    return KVError{KVErrorCodes::NoError, {}};
}
} // namespace kv
} // namespace gg
} // namespace aws