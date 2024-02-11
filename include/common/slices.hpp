#pragma once
#include <cstring>
#include <memory>
#include <string>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    class BorrowedSlice {
      private:
        const uint8_t *_data;
        const uint32_t _size;

      public:
        BorrowedSlice(const uint8_t *data, const uint32_t size) : _data(data), _size(size){};
        BorrowedSlice(const void *data, const uint32_t size) : _data(static_cast<const uint8_t *>(data)), _size(size){};
        BorrowedSlice(const char *data, const uint32_t size)
            : _data(reinterpret_cast<const uint8_t *>(data)), _size(size){};
        explicit BorrowedSlice(const std::string &s)
            : _data(reinterpret_cast<const uint8_t *>(s.data())), _size(s.length()){};

        [[nodiscard]] const uint8_t *data() const { return _data; };

        [[nodiscard]] const char *char_data() const { return reinterpret_cast<const char *>(_data); };

        [[nodiscard]] uint32_t size() const { return _size; };

        [[nodiscard]] std::string string() const { return {char_data(), _size}; };
    };

    class OwnedSlice : private std::unique_ptr<uint8_t[]> {
      private:
        uint32_t _size{0};

      public:
        OwnedSlice() = default;
        explicit OwnedSlice(BorrowedSlice b) : _size(b.size()) {
            std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
            memcpy(mem.get(), b.data(), b.size());
            swap(mem);
        }

        explicit OwnedSlice(uint32_t size) : _size(size) {
            std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
            swap(mem);
        }

        OwnedSlice(uint8_t d[], const uint32_t size) : _size(size) { reset(d); };

        OwnedSlice(OwnedSlice &&) = default;
        OwnedSlice(OwnedSlice &) = delete;
        OwnedSlice &operator=(OwnedSlice &) = delete;
        OwnedSlice &operator=(OwnedSlice &&) = default;

        ~OwnedSlice() = default;

        [[nodiscard]] uint8_t *data() const { return get(); };

        [[nodiscard]] const char *char_data() const { return reinterpret_cast<const char *>(get()); };

        [[nodiscard]] uint32_t size() const { return _size; };

        [[nodiscard]] std::string string() const { return {char_data(), _size}; };
    };
} // namespace gg
} // namespace aws