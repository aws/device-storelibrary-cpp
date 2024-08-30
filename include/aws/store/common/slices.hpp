// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#pragma once
#include <cassert>
#include <cstring>
#include <memory> // for unique_ptr
#include <string>

namespace aws {
namespace store {
namespace common {
class BorrowedSlice {
  private:
    const void *_data;
    const uint32_t _size;

  public:
    BorrowedSlice() : _data(nullptr), _size(0U) {};
    BorrowedSlice(const void *data, const size_t size) : _data(data), _size(static_cast<uint32_t>(size)) {
        // coverity[misra_cpp_2008_rule_5_2_12_violation] false positive
        assert(size <= UINT32_MAX);
    }
    explicit BorrowedSlice(const std::string &s) : _data(s.data()), _size(static_cast<uint32_t>(s.length())) {
        // coverity[misra_cpp_2008_rule_5_2_12_violation] false positive
        assert(s.length() <= UINT32_MAX);
    }

    const void *data() const {
        return _data;
    }

    const char *char_data() const {
        return static_cast<const char *>(_data);
    }

    uint32_t size() const {
        return _size;
    }

    std::string string() const {
        const auto d = char_data();
        return d == nullptr ? std::string{} : std::string{d, _size};
    }

    static_assert(sizeof(uint8_t) == sizeof(char), "Char and uint8 must be the same size");
};

class OwnedSlice : private std::unique_ptr<uint8_t[]> {
  private:
    uint32_t _size{0U};

  public:
    OwnedSlice() = default;
    explicit OwnedSlice(const BorrowedSlice b) : _size(b.size()) {
        // coverity[autosar_cpp14_a20_8_5_violation] cannot construct arbitrary size with make_unique
        // coverity[misra_cpp_2008_rule_18_4_1_violation] cannot construct arbitrary size with make_unique
        std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
        std::ignore = memcpy(mem.get(), b.data(), b.size());
        swap(mem);
    }

    explicit OwnedSlice(const uint32_t size) : _size(size) {
        // coverity[autosar_cpp14_a20_8_5_violation] cannot construct arbitrary size with make_unique
        // coverity[misra_cpp_2008_rule_18_4_1_violation] cannot construct arbitrary size with make_unique
        std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
        swap(mem);
    }

    OwnedSlice(uint8_t *d, const uint32_t size) : _size(size) {
        reset(d);
    }

    OwnedSlice(OwnedSlice &&) = default;
    OwnedSlice(OwnedSlice &) = delete;
    OwnedSlice &operator=(OwnedSlice &) = delete;
    OwnedSlice &operator=(OwnedSlice &&) = default;

    ~OwnedSlice() = default;

    void *data() const {
        return get();
    }

    const char *char_data() const {
        return reinterpret_cast<const char *>(get());
    }

    uint32_t size() const {
        return _size;
    }

    std::string string() const {
        const auto d = char_data();
        return d == nullptr ? std::string{} : std::string{d, _size};
    }
};
} // namespace common
} // namespace store
} // namespace aws
