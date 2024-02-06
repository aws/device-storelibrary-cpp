#include "kv.hpp"
#include "crc32.hpp"
#include <iostream>

namespace aws {
namespace gg {
constexpr static uint8_t DELETED_FLAG = 0x01;

struct KVHeader {
    uint8_t flags;
    uint32_t crc32;
    uint16_t key_length;
    uint16_t value_length;
};

KVError KV::initialize() {
    // Read from main file if it exists. If it doesn't exist, use the shadow file if available.
    if (_filesystem_implementation->exists(_name)) {
        _filesystem_implementation->remove(_shadow_name);
    } else if (_filesystem_implementation->exists(_shadow_name)) {
        _filesystem_implementation->rename(_shadow_name, _name);
    }

    auto e = _filesystem_implementation->open(_name);
    if (!e) {
        return KVError{KVErrorCodes::ReadError, e.err().msg};
    }
    _f = std::move(e.val());

    while (true) {
        uint32_t beginning_pointer = _byte_position;
        auto header_or = _f->read(_byte_position, _byte_position + sizeof(KVHeader));
        if (!header_or) {
            if (header_or.err().code == FileErrorCode::EndOfFile) {
                return KVError{KVErrorCodes::NoError, {}};
            }
            return KVError{KVErrorCodes::ReadError, "Incomplete header read. " + header_or.err().msg};
        }
        _byte_position += sizeof(KVHeader);
        auto *header = reinterpret_cast<const KVHeader *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            header_or.val().data());
        if (header->flags & DELETED_FLAG) {
            continue;
        }

        if (header->key_length > 0) {
            auto key_or = _f->read(_byte_position, _byte_position + header->key_length);
            if (!key_or) {
                return KVError{KVErrorCodes::ReadError, "Incomplete key read. " + key_or.err().msg};
            }
            _byte_position += header->key_length;

            auto value_or = _f->read(_byte_position, _byte_position + header->value_length);
            if (!value_or) {
                return KVError{KVErrorCodes::ReadError, "Incomplete value read. " + value_or.err().msg};
            }
            _byte_position += header->value_length;

            _key_pointers.emplace_back(key_or.val().string(), beginning_pointer);
        }
    }

    for (const auto &point : _key_pointers) {
        std::cout << point.first << " " << point.second << std::endl;
    }
    return KVError{KVErrorCodes::NoError, {}};
}

expected<OwnedSlice, KVError> KV::readFrom(const uint32_t begin) {
    auto header_or = _f->read(begin, begin + sizeof(KVHeader));
    if (!header_or) {
        return KVError{KVErrorCodes::ReadError, header_or.err().msg};
    }
    auto *header = reinterpret_cast<const KVHeader *>( // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        header_or.val().data());
    auto key_or = _f->read(begin + sizeof(KVHeader), begin + sizeof(KVHeader) + header->key_length);
    if (!key_or) {
        return KVError{KVErrorCodes::ReadError, key_or.err().msg};
    }
    auto value_or = _f->read(begin + sizeof(KVHeader) + header->key_length,
                             begin + sizeof(KVHeader) + header->key_length + header->value_length);
    if (!value_or) {
        return KVError{KVErrorCodes::ReadError, value_or.err().msg};
    }
    // TODO: Check CRC
    return std::move(value_or.val());
}

expected<OwnedSlice, KVError> KV::get(const std::string &key) {
    for (const auto &point : _key_pointers) {
        if (point.first == key) {
            return readFrom(point.second);
        }
    }
    return KVError{KVErrorCodes::KeyNotFound, {}};
}

KVError KV::put(const std::string &key, BorrowedSlice data) {
    auto key_len = static_cast<uint16_t>(key.length());
    auto value_len = static_cast<uint16_t>(data.size());
    uint8_t flags = 0;
    auto header = KVHeader{
        .flags = flags,
        .crc32 = crc32::update(
            crc32::update(crc32::update(crc32::update(0, &flags, sizeof(flags)), &key_len, sizeof(key_len)), &value_len,
                          sizeof(value_len)),
            data.data(), data.size()),
        .key_length = key_len,
        .value_length = value_len,
    };
    _f->append(
        BorrowedSlice(reinterpret_cast<const uint8_t *>(&header), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(header)));
    _f->append(BorrowedSlice(
        reinterpret_cast<const uint8_t *>(key.data()), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        key.length()));
    _f->append(data);

    bool found = false;
    for (auto &point : _key_pointers) {
        if (point.first == key) {
            point.second = _byte_position;
            found = true;
            break;
        }
    }

    if (!found) {
        _key_pointers.emplace_back(key, _byte_position);
    }

    _byte_position += sizeof(header) + key_len + value_len;

    return KVError{KVErrorCodes::NoError, {}};

    // TODO: rollover to shadow
}

KVError KV::remove(const std::string &key) {
    for (size_t i = 0; i < _key_pointers.size(); i++) {
        if (_key_pointers[i].first == key) {
            auto it = _key_pointers.begin();
            std::advance(it, i);
            _key_pointers.erase(it);
            break;
        }
    }

    uint16_t key_len = 0;
    uint16_t value_len = 0;
    uint8_t flags = DELETED_FLAG;
    auto header = KVHeader{
        .flags = flags,
        .crc32 = crc32::update(crc32::update(crc32::update(0, &flags, sizeof(flags)), &key_len, sizeof(key_len)),
                               &value_len, sizeof(value_len)),
        .key_length = key_len,
        .value_length = value_len,
    };
    _f->append(
        BorrowedSlice(reinterpret_cast<const uint8_t *>(&header), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                      sizeof(header)));

    _byte_position += sizeof(header);

    return KVError{KVErrorCodes::NoError, {}};
}
} // namespace gg
} // namespace aws