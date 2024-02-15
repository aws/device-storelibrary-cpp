#pragma once

#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

#include "common/crc32.hpp"
#include "common/logging.hpp"
#include "common/slices.hpp"
#include "filesystem/filesystem.hpp"

namespace aws {
namespace gg {
namespace kv __attribute__((visibility("default"))) {
    namespace detail {
    constexpr uint8_t VERSION = 0x01;
    constexpr uint8_t MAGIC = 0xB0;
    constexpr uint8_t MAGIC_AND_VERSION = static_cast<uint8_t>(MAGIC << 4 | VERSION);

    using value_length_type = uint32_t;
    using key_length_type = uint16_t;
    constexpr auto VALUE_LENGTH_MAX = UINT32_MAX;
    constexpr auto KEY_LENGTH_MAX = UINT16_MAX;

#pragma pack(push, 4)
    struct KVHeader {
        uint8_t magic_and_version{MAGIC_AND_VERSION};
        uint8_t flags{0};
        key_length_type key_length{0};
        uint32_t crc32{0};
        value_length_type value_length{0};
    };
#pragma pack(pop)

    } // namespace detail

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
        bool full_corruption_check_on_open{false};
        const std::shared_ptr<FileSystemInterface> filesystem_implementation;
        const std::shared_ptr<logging::Logger> logger;
        std::string identifier;
        // 0 means compact immediately when compaction would help. Negative numbers means to never automatically
        // compact. Positive number means to compact when it would save approximately that number of bytes.
        int32_t compact_after;
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

        [[nodiscard]] expected<detail::KVHeader, KVError> readHeaderFrom(uint32_t) const noexcept;

        [[nodiscard]] expected<std::string, KVError> readKeyFrom(uint32_t, detail::key_length_type) const noexcept;

        [[nodiscard]] expected<OwnedSlice, KVError> readValueFrom(uint32_t, detail::key_length_type,
                                                                  detail::value_length_type) const noexcept;

        [[nodiscard]] expected<OwnedSlice, KVError> readValueFrom(uint32_t) const noexcept;

        [[nodiscard]] expected<uint32_t, KVError> readWrite(uint32_t, std::pair<std::string, uint32_t> &,
                                                            FileLike &) noexcept;

        KV(KVOptions &&opts) noexcept : _opts(std::move(opts)), _shadow_name(_opts.identifier + "s") {}

        [[nodiscard]] KVError initialize() noexcept;

        [[nodiscard]] KVError compactNoLock() noexcept;

        bool removeKey(const std::string &key) noexcept;

        void truncateAndLog(uint32_t truncate, const KVError &) const noexcept;

        [[nodiscard]] KVError openFile() noexcept;

        void inline addOrRemoveKeyInInitialization(const std::string &key, uint32_t beginning_pointer,
                                                   uint32_t added_size, uint8_t flags) noexcept;

        [[nodiscard]] inline KVError writeEntry(const std::string &key, BorrowedSlice data,
                                                uint8_t flags) const noexcept;

        template <typename... Args> [[nodiscard]] inline FileError appendMultiple(const Args &...args) const noexcept;

        [[nodiscard]] KVError maybeCompact() noexcept;

      public:
        [[nodiscard]] static expected<std::shared_ptr<KV>, KVError> openOrCreate(KVOptions &&) noexcept;

        [[nodiscard]] expected<OwnedSlice, KVError> get(const std::string &) const noexcept;

        [[nodiscard]] KVError put(const std::string &, BorrowedSlice) noexcept;

        [[nodiscard]] KVError remove(const std::string &) noexcept;

        [[nodiscard]] expected<std::vector<std::string>, KVError> listKeys() const noexcept;

        [[nodiscard]] KVError compact() noexcept {
            std::lock_guard<std::mutex> lock(_lock);
            return compactNoLock();
        }

        [[nodiscard]] std::uint32_t currentSizeBytes() const noexcept { return _byte_position; }
    };
} // namespace kv
} // namespace gg
} // namespace aws