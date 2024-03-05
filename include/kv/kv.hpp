#pragma once

#include "common/expected.hpp"
#include "common/logging.hpp"
#include "common/slices.hpp"
#include "common/util.hpp"
#include "filesystem/filesystem.hpp"
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace aws {
namespace gg {
namespace kv __attribute__((visibility("default"))) {
    namespace detail {
    constexpr uint8_t MAGIC_AND_VERSION = 0xB1U;

    using value_length_type = uint32_t;
    using key_length_type = uint16_t;
    constexpr auto VALUE_LENGTH_MAX = UINT32_MAX / 2U;
    constexpr uint16_t KEY_LENGTH_MAX = 0xFFFFU;

#pragma pack(push, 4)
    struct KVHeader {
        uint8_t magic_and_version{MAGIC_AND_VERSION};
        uint8_t flags{0U};
        key_length_type key_length{0U};
        uint32_t crc32{0U};
        value_length_type value_length{0U};
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
        DiskFull,
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
        std::uint32_t _byte_position{0U};
        std::uint32_t _added_bytes{0U};
        mutable std::mutex _lock{};

        expected<detail::KVHeader, KVError> readHeaderFrom(const uint32_t) const noexcept;

        expected<std::string, KVError> readKeyFrom(const uint32_t, const detail::key_length_type) const noexcept;

        expected<OwnedSlice, KVError> readValueFrom(const uint32_t, const detail::KVHeader &) const noexcept;

        expected<OwnedSlice, KVError> readValueFrom(const uint32_t) const noexcept;

        expected<uint32_t, KVError> readWrite(const uint32_t, std::pair<std::string, uint32_t> &, FileLike &) noexcept;

        KV(KVOptions &&opts) noexcept : _opts(std::move(opts)), _shadow_name(_opts.identifier + "s") {
        }

        KVError initialize() noexcept;

        KVError compactNoLock() noexcept;

        bool removeKey(const std::string &key) noexcept;

        void truncateAndLog(const uint32_t truncate, const KVError &) const noexcept;

        KVError openFile() noexcept;

        inline void addOrRemoveKeyInInitialization(const std::string &key, const uint32_t beginning_pointer,
                                                   const uint32_t added_size, const uint8_t flags) noexcept;

        inline KVError writeEntry(const std::string &key, const BorrowedSlice data, const uint8_t flags) const noexcept;

        template <typename... Args> FileError appendMultiple(const Args &...args) const noexcept;

        KVError maybeCompact() noexcept;

      public:
        static expected<std::shared_ptr<KV>, KVError> openOrCreate(KVOptions &&) noexcept;

        expected<OwnedSlice, KVError> get(const std::string &) const noexcept;

        KVError put(const std::string &, const BorrowedSlice) noexcept;

        KVError remove(const std::string &) noexcept;

        expected<std::vector<std::string>, KVError> listKeys() const noexcept;

        KVError compact() noexcept {
            std::lock_guard<std::mutex> lock(_lock);
            return compactNoLock();
        }

        std::uint32_t currentSizeBytes() const noexcept {
            return _byte_position;
        }
    };
} // namespace kv
} // namespace gg
} // namespace aws