#pragma once

#include "aws/store/common/expected.hpp"
#include "aws/store/common/logging.hpp"
#include "aws/store/common/slices.hpp"
#include "aws/store/common/util.hpp"
#include "aws/store/filesystem/filesystem.hpp"
#include "aws/store/kv/kv.hpp"
#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

#if __cplusplus >= 201703L
#define WEAK_FROM_THIS weak_from_this
#else
#define WEAK_FROM_THIS shared_from_this
#endif

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    struct OwnedRecord {
        uint32_t offset{};
        OwnedSlice data{};
        int64_t timestamp{};
        uint64_t sequence_number{};

        OwnedRecord() = default;
        OwnedRecord(OwnedSlice &&idata, const int64_t itimestamp, const uint64_t isequence_number,
                    const uint32_t ioffset) noexcept
            : offset(ioffset), data(std::move(idata)), timestamp(itimestamp), sequence_number(isequence_number){};
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
    using StreamError = GenericError<StreamErrorCode>;

    class CheckpointableOwnedRecord : public OwnedRecord {
      private:
        std::function<void(void)> _checkpoint;

      public:
        CheckpointableOwnedRecord() = default;
        CheckpointableOwnedRecord(OwnedRecord &&o, std::function<StreamError(void)> &&checkpoint) noexcept
            : OwnedRecord(std::move(o)), _checkpoint(std::move(checkpoint)){};
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &) = delete;
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &&) = default;
        ~CheckpointableOwnedRecord() = default;

        CheckpointableOwnedRecord &operator=(CheckpointableOwnedRecord &&o) = default;
        CheckpointableOwnedRecord &operator=(CheckpointableOwnedRecord &) = delete;

        void checkpoint() const noexcept {
            _checkpoint();
        }
    };

    class Iterator {
      private:
        std::weak_ptr<StreamInterface> _stream;
        std::string _id;
        uint32_t _offset = 0U;

        StreamError checkpoint() const noexcept;

      public:
        explicit Iterator(std::weak_ptr<StreamInterface> s, std::string id, const uint64_t seq) noexcept
            : _stream(std::move(s)), _id(std::move(id)), sequence_number(seq){};

        Iterator(Iterator &) = delete;

        Iterator &operator=(Iterator &) = delete;

        Iterator(Iterator &&) = default;

        Iterator &operator=(Iterator &&) = default;
        ~Iterator() = default;

        int64_t timestamp = 0;
        uint64_t sequence_number = 0U;

        // mutate in place and return this
        Iterator &operator++() noexcept {
            ++sequence_number;
            timestamp = 0;
            return *this;
        }

        expected<CheckpointableOwnedRecord, StreamError> operator*() noexcept;

        Iterator &&begin() noexcept {
            return std::move(*this);
        }

        /**
         * The iterator never ends.
         */
        static int end() noexcept {
            return 0;
        }

        bool operator!=(const int x) const noexcept {
            static_cast<void>(x); // unused parameter required as part of interface
            return true;
        }
    };

    struct ReadOptions {
        bool check_for_corruption{true};
        bool may_return_later_records{false};
        std::uint32_t suggested_start{0U};
    };

    struct AppendOptions {
        bool sync_on_append{false};
        bool remove_oldest_segments_if_full{true};
    };

    class StreamInterface : public std::enable_shared_from_this<StreamInterface> {
      protected:
        std::atomic_uint64_t _first_sequence_number{0U};
        std::atomic_uint64_t _next_sequence_number{0U};
        std::atomic_uint64_t _current_size_bytes{0U};

      public:
        std::uint64_t firstSequenceNumber() const noexcept {
            return _first_sequence_number;
        };
        std::uint64_t highestSequenceNumber() const noexcept {
            return _next_sequence_number - 1U;
        };
        std::uint64_t currentSizeBytes() const noexcept {
            return _current_size_bytes;
        };
        StreamInterface(StreamInterface &) = delete;

        /**
         * Append data into the stream.
         *
         * @return the sequence number of the record appended.
         */
        virtual expected<uint64_t, StreamError> append(const BorrowedSlice, const AppendOptions &) noexcept = 0;

        /**
         * Append data into the stream.
         *
         * @return the sequence number of the record appended.
         */
        virtual expected<uint64_t, StreamError> append(OwnedSlice &&, const AppendOptions &) noexcept = 0;

        /**
         * Read a record from the stream by its sequence number or an error.
         *
         * @param sequence_number the sequence number of the record to read.
         * @return the Record.
         */
        virtual expected<OwnedRecord, StreamError> read(const uint64_t sequence_number,
                                                        const ReadOptions &) const noexcept = 0;

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
        const std::shared_ptr<FileSystemInterface> file_implementation{};
        const std::shared_ptr<logging::Logger> logger{};
        kv::KVOptions kv_options = {false, file_implementation, logger, "kv", 128 * 1024};
    };

    inline expected<CheckpointableOwnedRecord, StreamError> Iterator::operator*() noexcept {
        if (const auto stream = _stream.lock()) {
            auto record_or = stream->read(sequence_number, ReadOptions{true, true, _offset});
            if (!record_or.ok()) {
                return record_or.err();
            }
            auto x = std::move(record_or.val());
            timestamp = x.timestamp;
            _offset = x.offset + x.data.size();
            sequence_number = x.sequence_number;
            // coverity[autosar_cpp14_a7_1_7_violation] defining lambda inline
            return CheckpointableOwnedRecord{std::move(x), [this]() -> StreamError { return checkpoint(); }};
        }
        return StreamError{StreamErrorCode::StreamClosed, "Unable to read from destroyed stream"};
    }

    inline StreamError Iterator::checkpoint() const noexcept {
        auto e = StreamError{StreamErrorCode::StreamClosed, "Unable to read from destroyed stream"};
        if (const auto stream = _stream.lock()) {
            e = stream->setCheckpoint(_id, sequence_number);
        }
        return e;
    }

    inline int64_t timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    static constexpr auto RecordNotFoundErrorStr = "Record not found";
} // namespace gg
} // namespace aws