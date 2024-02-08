#pragma once

#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

#include "common/slices.hpp"
#include "filesystem/filesystem.hpp"

namespace aws {
namespace gg {
namespace kv __attribute__((visibility("default"))) {
    enum class KVErrorCodes : std::uint8_t {
        NoError,
        KeyNotFound,
        ReadError,
        WriteError,
        HeaderCorrupted,
        DataCorrupted,
        EndOfFile,
        InvalidArguments,
        Unknown,
    };
    using KVError = GenericError<KVErrorCodes>;

    struct KVOptions {
        std::shared_ptr<FileSystemInterface> filesystem_implementation;
        std::string identifier;
        uint32_t compact_after;
    };

    constexpr uint8_t VERSION = 0x01;
    constexpr uint8_t MAGIC = 0xB0;
    constexpr uint8_t MAGIC_AND_VERSION = static_cast<uint8_t>(MAGIC << 4 | VERSION);

    using value_length_type = uint32_t;
    using key_length_type = uint16_t;
    constexpr auto VALUE_LENGTH_MAX = UINT32_MAX;
    constexpr auto KEY_LENGTH_MAX = UINT16_MAX;

    struct KVHeader {
        uint8_t magic_and_version{MAGIC_AND_VERSION};
        uint8_t flags{0};
        key_length_type key_length{0};
        uint32_t crc32{0};
        value_length_type value_length{0};
    };

    class KV {
      private:
        KVOptions _opts;
        std::string _shadow_name;
        std::vector<std::pair<std::string, uint32_t>> _key_pointers{};
        std::unique_ptr<FileLike> _f{nullptr};
        std::uint32_t _byte_position{0};
        std::uint32_t _added_bytes{0};
        mutable std::mutex _lock{};

        [[nodiscard]] expected<KVHeader, KVError> readHeaderFrom(uint32_t) const;
        [[nodiscard]] expected<std::string, KVError> readKeyFrom(uint32_t, key_length_type) const;
        [[nodiscard]] expected<OwnedSlice, KVError> readValueFrom(uint32_t, key_length_type, value_length_type) const;
        [[nodiscard]] expected<OwnedSlice, KVError> readValueFrom(uint32_t) const;

        [[nodiscard]] KVError readWrite(std::pair<std::string, uint32_t> &, FileLike &);

        KV(KVOptions &&opts) : _opts(std::move(opts)), _shadow_name(_opts.identifier + "s") {}

        [[nodiscard]] KVError initialize();
        [[nodiscard]] KVError compactNoLock();
        [[nodiscard]] bool removeKey(const std::string &key);

      public:
        [[nodiscard]] static expected<std::shared_ptr<KV>, KVError> openOrCreate(KVOptions &&);

        [[nodiscard]] expected<OwnedSlice, KVError> get(const std::string &) const;
        [[nodiscard]] KVError put(const std::string &, BorrowedSlice);
        [[nodiscard]] KVError remove(const std::string &);
        [[nodiscard]] expected<std::vector<std::string>, KVError> listKeys() const;
        [[nodiscard]] KVError compact() {
            std::lock_guard<std::mutex> lock(_lock);
            return compactNoLock();
        }
    };
} // namespace kv
} // namespace gg
} // namespace aws