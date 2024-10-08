// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <aws/store/common/expected.hpp>
#include <aws/store/common/logging.hpp>
#include <aws/store/common/slices.hpp>
#include <aws/store/common/util.hpp>
#include <aws/store/filesystem/filesystem.hpp>
#include <aws/store/kv/kv.hpp>
#include <cstdint>
#include <functional>
#include <string>

#if __cplusplus >= 201703L
#define WEAK_FROM_THIS weak_from_this
#else
#define WEAK_FROM_THIS shared_from_this
#endif

namespace aws {
namespace store {
namespace stream __attribute__((visibility("default"))) {
    struct OwnedRecord {
        uint32_t offset{};
        common::OwnedSlice data{};
        int64_t timestamp{};
        uint64_t sequence_number{};

        OwnedRecord() = default;

        // coverity[autosar_cpp14_a15_4_3_violation] false positive, all implementations are noexcept
        // coverity[misra_cpp_2008_rule_15_4_1_violation] false positive, implementation is noexcept
        OwnedRecord(common::OwnedSlice &&idata, const int64_t itimestamp, const uint64_t isequence_number,
                    const uint32_t ioffset) noexcept;
    };

    enum class StreamErrorCode : std::uint8_t {
        NoError,
        RecordNotFound,
        RecordDataCorrupted,
        HeaderDataCorrupted,
        RecordTooLarge,
        ReadError,
        WriteError,
        StreamClosed,
        InvalidArguments,
        DiskFull,
        IteratorNotFound,
        StreamFull,
        Unknown,
    };

    struct IteratorOptions {};

    class StreamInterface;
    using StreamError = common::GenericError<StreamErrorCode>;

    class CheckpointableOwnedRecord : public OwnedRecord {
      private:
        std::function<StreamError(void)> _checkpoint;

      public:
        CheckpointableOwnedRecord() = default;

        // coverity[autosar_cpp14_a15_4_3_violation] false positive, all implementations are noexcept
        // coverity[misra_cpp_2008_rule_15_4_1_violation] false positive, implementation is noexcept
        CheckpointableOwnedRecord(OwnedRecord &&o, std::function<StreamError(void)> &&checkpoint) noexcept;
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &) = delete;
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &&) = default;
        ~CheckpointableOwnedRecord() = default;

        CheckpointableOwnedRecord &operator=(CheckpointableOwnedRecord &&o) = default;
        CheckpointableOwnedRecord &operator=(CheckpointableOwnedRecord &) = delete;

        StreamError checkpoint() const noexcept;
    };

    class Iterator {
      private:
        std::weak_ptr<StreamInterface> _stream;
        std::string _id;
        uint32_t _offset = 0U;

      public:
        // coverity[autosar_cpp14_a15_4_3_violation] false positive, all implementations are noexcept
        // coverity[misra_cpp_2008_rule_15_4_1_violation] false positive, implementation is noexcept
        explicit Iterator(std::weak_ptr<StreamInterface> s, std::string id, const uint64_t seq) noexcept;

        Iterator(Iterator &) = delete;

        Iterator &operator=(Iterator &) = delete;

        Iterator(Iterator &&) = default;

        Iterator &operator=(Iterator &&) = default;
        ~Iterator() = default;

        int64_t timestamp = 0;
        uint64_t sequence_number = 0U;

        /*
         * mutate in place and return this
         */
        // coverity[autosar_cpp14_a15_4_3_violation] false positive, all implementations are noexcept
        // coverity[misra_cpp_2008_rule_15_4_1_violation] false positive, implementation is noexcept
        Iterator &operator++() noexcept;

        // coverity[autosar_cpp14_a15_4_3_violation] false positive, all implementations are noexcept
        // coverity[misra_cpp_2008_rule_15_4_1_violation] false positive, implementation is noexcept
        common::Expected<CheckpointableOwnedRecord, StreamError> operator*() noexcept;

        Iterator &&begin() noexcept;

        Iterator end() noexcept;

        // coverity[autosar_cpp14_a15_4_3_violation] false positive, all implementations are noexcept
        // coverity[misra_cpp_2008_rule_15_4_1_violation] false positive, implementation is noexcept
        bool operator!=(const Iterator &x) const noexcept;
    };

    struct ReadOptions {
        bool check_for_corruption;
        bool may_return_later_records;
        std::uint32_t suggested_start;

        ~ReadOptions() = default;
        ReadOptions(const ReadOptions &) = default;
        ReadOptions(ReadOptions &&) = default;
        ReadOptions &operator=(const ReadOptions &) = default;
        ReadOptions &operator=(ReadOptions &&) = default;

        ReadOptions(bool check_for_corruption_opt = true, bool may_return_later_records_opt = false,
                    std::uint32_t suggested_start_opt = 0U);
    };

    struct AppendOptions {
        bool sync_on_append;
        bool remove_oldest_segments_if_full;

        ~AppendOptions() = default;
        AppendOptions(const AppendOptions &) = default;
        AppendOptions(AppendOptions &&) = default;
        AppendOptions &operator=(const AppendOptions &) = default;
        AppendOptions &operator=(AppendOptions &&) = default;

        AppendOptions(bool sync_on_append_opt = false, bool remove_oldest_segments_if_full_opt = true);
    };

    class StreamInterface : public std::enable_shared_from_this<StreamInterface> {
      protected:
        std::atomic_uint64_t _first_sequence_number{0U};
        std::atomic_uint64_t _next_sequence_number{0U};
        std::atomic_uint64_t _current_size_bytes{0U};

      public:
        std::uint64_t firstSequenceNumber() const noexcept;
        std::uint64_t highestSequenceNumber() const noexcept;
        std::uint64_t currentSizeBytes() const noexcept;
        StreamInterface(StreamInterface &) = delete;

        /**
         * Append data into the stream.
         *
         * @return the sequence number of the record appended.
         */
        virtual common::Expected<uint64_t, StreamError> append(const common::BorrowedSlice,
                                                               const AppendOptions &) noexcept = 0;

        /**
         * Append data into the stream.
         *
         * @return the sequence number of the record appended.
         */
        virtual common::Expected<uint64_t, StreamError> append(common::OwnedSlice &&,
                                                               const AppendOptions &) noexcept = 0;

        /**
         * Read a record from the stream by its sequence number or an error.
         *
         * @param sequence_number the sequence number of the record to read.
         * @return the Record.
         */
        virtual common::Expected<OwnedRecord, StreamError> read(const uint64_t sequence_number,
                                                                const ReadOptions &) const noexcept = 0;

        /**
         * Attempt to remove records from the stream that are older than the provided timestamp.
         *
         * This operation is a best-effort, and it is not guaranteed that ALL older records will be
         * removed.
         * For example, the file stream implementation is based on segments (groups of records),
         * and segments will only be deleted if all of their records are expired.
         *
         * @param older_than_timestamp_ms timestamp for which stream records will be compared against
         */
        virtual uint64_t removeOlderRecords(int64_t older_than_timestamp_ms) noexcept = 0;

        /**
         * Create an iterator identified by the chosen identifier. If an iterator already exists with the same
         * identifier, this iterator will start from the last record checkpoint() was called for the same identifier.
         * If no iterator exists with the same identifier, this iterator will start from the beginning of the stream.
         * The iterator will be persisted until explicitly deleted using deleteIterator().
         *
         * @param identifier an alphanumeric value to uniquely identify this iterator. Provide the same identifier later
         * on to resume from the last checkpoint.
         * @return the iterator.
         */
        virtual Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) noexcept = 0;

        /**
         * Delete the persisted iterator if present. Nop if no iterator exists with the given identifier.
         *
         * @param identifier identifier of the iterator to delete.
         */
        virtual StreamError deleteIterator(const std::string &identifier) noexcept = 0;

        /**
         * Persist a checkpoint for the iterator identified at the given sequence number.
         *
         * @param identifier identifier of the iterator to associate with this checkpoint.
         * @param sequence_number sequence number of the record. When opening an existing iterator it will start from
         * this record.
         */
        virtual StreamError setCheckpoint(const std::string &identifier, const uint64_t sequence_number) noexcept = 0;

        StreamInterface() noexcept = default;

        virtual ~StreamInterface() noexcept = default;
    };

    struct StreamOptions {
        uint32_t minimum_segment_size_bytes =
            16U * 1024U * 1024U;                            // 16MB minimum segment size before making a new segment
        uint32_t maximum_size_bytes = 128U * 1024U * 1024U; // 128MB max stream size
        bool full_corruption_check_on_open = false;
        const std::shared_ptr<filesystem::FileSystemInterface> file_implementation{};
        const std::shared_ptr<logging::Logger> logger{};
        kv::KVOptions kv_options = {false, file_implementation, logger, "kv", 128 * 1024};
    };

    int64_t timestamp() noexcept;

    static constexpr auto RecordNotFoundErrorStr = "Record not found";
} // namespace stream
} // namespace store
} // namespace aws
