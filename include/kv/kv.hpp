#pragma once

#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

#include "common/slices.hpp"
#include "filesystem/filesystem.hpp"

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

    struct KVOptions {
        std::shared_ptr<FileSystemInterface> filesystem_implementation;
        std::string identifier;
        uint32_t compact_after;
    };

    struct KVHeader {
        uint8_t flags;
        uint32_t crc32;
        uint16_t key_length;
        uint16_t value_length;
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

        [[nodiscard]] expected<KVHeader, FileError> readHeaderFrom(uint32_t) const;
        [[nodiscard]] expected<std::string, FileError> readKeyFrom(uint32_t, uint16_t) const;
        [[nodiscard]] expected<OwnedSlice, FileError> readValueFrom(uint32_t, uint16_t, uint16_t) const;
        [[nodiscard]] expected<OwnedSlice, FileError> readValueFrom(uint32_t) const;

        [[nodiscard]] FileError readWrite(std::pair<std::string, uint32_t> &, FileLike &);

        KV(KVOptions &&opts) : _opts(std::move(opts)), _shadow_name(_opts.identifier + "s") {}

        [[nodiscard]] KVError initialize();
        [[nodiscard]] KVError compactNoLock();
        void removeKey(const std::string &key);

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
} // namespace gg
} // namespace aws