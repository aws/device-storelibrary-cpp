#pragma once

#include <utility>
#include <vector>
#include <cstdint>
#include <atomic>

#include "filesystem.hpp"
#include "slices.hpp"

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    enum class KVErrorCodes : std::uint8_t {
        NoError,
        KeyNotFound,
        ReadError,
        WriteError,
        Unknown,
    };
    using KVError = GenericError<KVErrorCodes>;

    class KV {
      private:
        std::string _name;
        std::string _shadow_name;
        std::shared_ptr<FileSystemInterface> _filesystem_implementation;
        std::vector<std::pair<std::string, uint32_t>> _key_pointers{};
        std::unique_ptr<FileLike> _f{nullptr};
        std::atomic_uint32_t _byte_position{0};

        [[nodiscard]] expected<OwnedSlice, KVError> readFrom(uint32_t);

      public:
        KV(std::shared_ptr<FileSystemInterface> filesystemI, std::string name)
            : _name(std::move(name)), _shadow_name(_name + "s"), _filesystem_implementation(std::move(filesystemI)) {}

        [[nodiscard]] KVError initialize();

        [[nodiscard]] expected<OwnedSlice, KVError> get(const std::string &);
        [[nodiscard]] KVError put(const std::string &, BorrowedSlice);
        [[nodiscard]] KVError remove(const std::string &);
    };
} // namespace gg
} // namespace aws