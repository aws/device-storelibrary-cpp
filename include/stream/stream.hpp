#pragma once

#include "common/expected.hpp"
#include "common/logging.hpp"
#include "common/slices.hpp"
#include "common/util.hpp"
#include "filesystem/filesystem.hpp"
#include "kv/kv.hpp"
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    struct OwnedRecord {
        OwnedSlice data{};
        int64_t timestamp{};
        uint64_t sequence_number{};
        uint64_t offset{};

        OwnedRecord() = default;
        OwnedRecord(OwnedSlice &&data, int64_t timestamp, uint64_t sequence_number, uint64_t offset)
            : data(std::move(data)), timestamp(timestamp), sequence_number(sequence_number), offset(offset){};
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
        Unknown,
    };

    struct IteratorOptions {};

    class StreamInterface;

    class CheckpointableOwnedRecord : public OwnedRecord {
      private:
        std::function<void(void)> _checkpoint;

      public:
        CheckpointableOwnedRecord() = default;
        CheckpointableOwnedRecord(OwnedRecord &&o, std::function<void(void)> &&checkpoint)
            : OwnedRecord(std::move(o)), _checkpoint(std::move(checkpoint)){};
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &) = delete;
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &&) = default;
        ~CheckpointableOwnedRecord() = default;

        CheckpointableOwnedRecord &operator=(CheckpointableOwnedRecord &&o) = default;
        CheckpointableOwnedRecord operator=(CheckpointableOwnedRecord &) = delete;

        void checkpoint() const { _checkpoint(); }
    };

    using StreamError = GenericError<StreamErrorCode>;

    class Iterator {
      private:
        std::weak_ptr<StreamInterface> _stream;
        std::string _id;
        uint64_t _offset = 0;

        StreamError checkpoint() const;

      public:
        explicit Iterator(std::weak_ptr<StreamInterface> s, std::string id, uint64_t seq)
            : _stream(std::move(s)), _id(std::move(id)), sequence_number(seq){};

        Iterator(Iterator &) = delete;

        Iterator &operator=(Iterator &) = delete;

        Iterator(Iterator &&) = default;

        Iterator &operator=(Iterator &&) = default;

        int64_t timestamp = 0;
        uint64_t sequence_number = 0;

        // mutate in place and return this
        Iterator &operator++() {
            ++sequence_number;
            timestamp = 0;
            return *this;
        }

        expected<CheckpointableOwnedRecord, StreamError> operator*();

        [[nodiscard]] Iterator &&begin() { return std::move(*this); }

        /**
         * The iterator never ends.
         */
        static int end() { return 0; }

        bool operator!=(const int) const { return true; }
    };

    struct ReadOptions {
        bool check_for_corruption{true};
        bool may_return_later_records{false};
        std::uint64_t suggested_start{0};
    };

#if __cplusplus >= 201703L
#define WEAK_FROM_THIS weak_from_this
#else
#define WEAK_FROM_THIS shared_from_this
#endif

    class StreamInterface : public std::enable_shared_from_this<StreamInterface> {
      protected:
        std::uint64_t _first_sequence_number{0};
        std::uint64_t _next_sequence_number{0};
        std::uint64_t _current_size_bytes{0};

      public:
        std::uint64_t firstSequenceNumber() { return _first_sequence_number; };
        std::uint64_t highestSequenceNumber() { return _next_sequence_number - 1; };
        std::uint64_t currentSizeBytes() { return _current_size_bytes; };
        StreamInterface(StreamInterface &) = delete;

        /**
         * Append data into the stream.
         *
         * @return the sequence number of the record appended.
         */
        virtual expected<uint64_t, StreamError> append(BorrowedSlice) = 0;

        /**
         * Append data into the stream.
         *
         * @return the sequence number of the record appended.
         */
        virtual expected<uint64_t, StreamError> append(OwnedSlice &&) = 0;

        /**
         * Read a record from the stream by its sequence number or an error.
         *
         * @param sequence_number the sequence number of the record to read.
         * @return the Record.
         */
        [[nodiscard]] virtual expected<OwnedRecord, StreamError> read(uint64_t sequence_number,
                                                                      const ReadOptions &) const = 0;

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
        [[nodiscard]] virtual Iterator openOrCreateIterator(const std::string &identifier, IteratorOptions) = 0;

        /**
         * Delete the persisted iterator if present. Nop if no iterator exists with the given identifier.
         *
         * @param identifier identifier of the iterator to delete.
         */
        virtual void deleteIterator(const std::string &identifier) = 0;

        /**
         * Persist a checkpoint for the iterator identified at the given sequence number.
         *
         * @param identifier identifier of the iterator to associate with this checkpoint.
         * @param sequence_number sequence number of the record. When opening an existing iterator it will start from
         * this record.
         */
        virtual void setCheckpoint(const std::string &identifier, uint64_t sequence_number) = 0;

        StreamInterface() = default;

        virtual ~StreamInterface() = default;
    };

    struct StreamOptions {
        size_t minimum_segment_size_bytes = 16 * 1024 * 1024; // 16MB minimum segment size before making a new segment
        size_t maximum_size_bytes = 128 * 1024 * 1024;        // 128MB max stream size
        bool full_corruption_check_on_open = false;
        const std::shared_ptr<FileSystemInterface> file_implementation{};
        const std::shared_ptr<logging::Logger> logger{};
        kv::KVOptions kv_options = {.filesystem_implementation = file_implementation,
                                    .logger = logger,
                                    .identifier = "kv",
                                    .compact_after = 128 * 1024};
    };

    inline expected<CheckpointableOwnedRecord, StreamError> Iterator::operator*() {
        if (auto stream = _stream.lock()) {
            auto record_or = stream->read(sequence_number, ReadOptions{.check_for_corruption = true,
                                                                       .may_return_later_records = true,
                                                                       .suggested_start = _offset});
            if (!record_or) {
                return std::move(record_or.err());
            }
            auto x = std::move(record_or.val());
            timestamp = x.timestamp;
            _offset = x.offset + x.data.size();
            sequence_number = x.sequence_number;
            return CheckpointableOwnedRecord{std::move(x), [this] { checkpoint(); }};
        }
        return StreamError{StreamErrorCode::StreamClosed, "Unable to read from destroyed stream"};
    }

    inline StreamError Iterator::checkpoint() const {
        if (auto stream = _stream.lock()) {
            stream->setCheckpoint(_id, sequence_number);
            return StreamError{StreamErrorCode::NoError, {}};
        }
        return StreamError{StreamErrorCode::StreamClosed, "Unable to read from destroyed stream"};
    }

    [[nodiscard]] inline int64_t timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }
} // namespace gg
} // namespace aws