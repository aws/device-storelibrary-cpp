#pragma once
#include "common/expected.hpp"
#include "common/slices.hpp"
#include "stream.hpp"
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    class MemoryStream : public StreamInterface {
      private:
        StreamOptions _opts;
        std::vector<OwnedRecord> _records{};
        std::unordered_map<std::string, uint64_t> _iterators{};

        explicit MemoryStream(StreamOptions &&o) noexcept : _opts(std::move(o)) {
        }

        StreamError remove_records_if_new_record_beyond_max_size(const uint32_t record_size) noexcept;

      public:
        static std::shared_ptr<MemoryStream> openOrCreate(StreamOptions &&) noexcept;

        expected<uint64_t, StreamError> append(const BorrowedSlice, const AppendOptions &) noexcept override;
        expected<uint64_t, StreamError> append(OwnedSlice &&, const AppendOptions &) noexcept override;

        expected<OwnedRecord, StreamError> read(const uint64_t sequence_number,
                                                const ReadOptions &) const noexcept override;

        Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept override;
        StreamError deleteIterator(const std::string &identifier) noexcept override;

        StreamError setCheckpoint(const std::string &, const uint64_t) noexcept override;

        ~MemoryStream() override = default;
    };
} // namespace gg
} // namespace aws