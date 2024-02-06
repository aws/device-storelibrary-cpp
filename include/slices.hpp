#pragma once
#include <memory>
#include <string>
#include <cstring>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    class BorrowedSlice {
      private:
        const uint8_t *_data;
        const size_t _size;

      public:
        BorrowedSlice(const uint8_t *data, const size_t size) : _data(data), _size(size){};

        [[nodiscard]] const uint8_t *data() const { return _data; };

        [[nodiscard]] size_t size() const { return _size; };

        [[nodiscard]] std::string string() const { return {reinterpret_cast<const char *>(_data), _size}; };
    };

    class OwnedSlice : private std::unique_ptr<uint8_t[]> {
      private:
        size_t _size;

      public:
        OwnedSlice() = default;
        explicit OwnedSlice(BorrowedSlice b) : _size(b.size()) {
            std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
            memcpy(mem.get(), b.data(), b.size());
            swap(mem);
        }

        explicit OwnedSlice(size_t size) : _size(size) {
            std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
            swap(mem);
        }

        OwnedSlice(uint8_t d[], const size_t size) : _size(size) { reset(d); };

        OwnedSlice(OwnedSlice &&) = default;
        OwnedSlice(OwnedSlice &) = delete;
        OwnedSlice operator=(OwnedSlice &) = delete;
        OwnedSlice &operator=(OwnedSlice &&) = default;

        ~OwnedSlice() = default;

        [[nodiscard]] uint8_t *data() const { return get(); };

        [[nodiscard]] size_t size() const { return _size; };

        [[nodiscard]] std::string string() const { return {reinterpret_cast<const char *>(get()), _size}; };
    };
} // namespace gg
} // namespace aws