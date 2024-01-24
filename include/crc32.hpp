#pragma once

#include <array>
#include <cstdint>
#include <cstdlib>

struct crc32 {
    constexpr static uint32_t crc32_polynomial = 0xEDB88320;

    static constexpr auto table = [] {
        auto crc32_table = std::array<uint32_t, 256>{};
        for (uint32_t byte = 0; byte < crc32_table.size(); ++byte) {
            uint32_t crc = byte;
            for (int i = 0; i < 8; ++i) {
                const auto m = crc & 1;
                crc >>= 1;
                if (m != 0) {
                    crc ^= crc32_polynomial;
                }
            }
            crc32_table[byte] = crc;
        }
        return crc32_table;
    }();

    [[nodiscard]] static uint32_t update(uint32_t initial_value, const void *buf, size_t len) {
        uint32_t c = initial_value ^ 0xFFFFFFFF;
        const auto *u = static_cast<const uint8_t *>(buf);
        for (size_t i = 0; i < len; ++i) {
            c = table[(c ^ u[i]) & 0xFF] ^ (c >> 8);
        }
        return c ^ 0xFFFFFFFF;
    }
};
