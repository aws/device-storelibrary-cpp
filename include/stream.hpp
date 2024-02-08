#pragma once

#include "expected.hpp"
#include "filesystem.hpp"
#include "kv.hpp"
#include "slices.hpp"
#include "util.hpp"
#include <atomic>
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
        OwnedSlice data;
        int64_t timestamp;
        uint64_t sequence_number;
        uint64_t offset;

        OwnedRecord() = default;
        OwnedRecord(OwnedSlice &&data, int64_t timestamp, uint64_t sequence_number, uint64_t offset)
            : data(std::move(data)), timestamp(timestamp), sequence_number(sequence_number), offset(offset){};

        OwnedRecord(OwnedRecord &&o) = default;
        OwnedRecord &operator=(OwnedRecord &&) = default;
        OwnedRecord(OwnedRecord &) = delete;
        OwnedRecord operator=(OwnedRecord &) = delete;

        ~OwnedRecord() = default;
    };

    enum class StreamErrorCode : std::uint8_t {
        NoError,
        RecordNotFound,
        RecordDataCorrupted,
        HeaderDataCorrupted,
        RecordTooLarge,
        ReadError,
        WriteError,
        Unknown,
    };

    struct IteratorOptions {};

    class StreamInterface;

    class CheckpointableOwnedRecord : public OwnedRecord {
      private:
        std::function<void(void)> _checkpoint;

      public:
        CheckpointableOwnedRecord(OwnedRecord &&o, std::function<void(void)> &&checkpoint)
            : OwnedRecord(std::move(o)), _checkpoint(std::move(checkpoint)){};
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &) = delete;
        CheckpointableOwnedRecord(CheckpointableOwnedRecord &&) = default;
        ~CheckpointableOwnedRecord() = default;

        CheckpointableOwnedRecord &operator=(CheckpointableOwnedRecord &&o) = default;
        CheckpointableOwnedRecord operator=(CheckpointableOwnedRecord &) = delete;

        void checkpoint() const { _checkpoint(); }
    };

    // Ensure non-copyable (or if copied, copy is a duplication rather than a new
    // independent iterator)
    class Iterator {
      private:
        std::weak_ptr<StreamInterface> _stream;
        std::string _id;
        uint64_t _offset = 0;

        void checkpoint() const;

      public:
        explicit Iterator(std::weak_ptr<StreamInterface> s, const std::string &id, uint64_t seq)
            : _stream(std::move(s)), _id(id), sequence_number(seq){};

        Iterator(Iterator &) = delete;

        Iterator &operator=(Iterator &) = delete;

        Iterator(Iterator &&) = default;

        Iterator &operator=(Iterator &&) = default;

        uint32_t flags = 0;
        int64_t timestamp = 0;
        uint64_t sequence_number = 0;

        // mutate in place and return this
        Iterator &operator++() {
            ++sequence_number;
            flags = 0;
            timestamp = 0;
            return *this;
        }

        CheckpointableOwnedRecord operator*();

        [[nodiscard]] Iterator &&begin() { return std::move(*this); }

        /**
         * The iterator never ends.
         */
        static int end() { return 0; }

        bool operator!=(const int) const { return true; }
    };

#if __cplusplus >= 201703L
#define WEAK_FROM_THIS weak_from_this
#else
#define WEAK_FROM_THIS shared_from_this
#endif

    using StreamError = GenericError<StreamErrorCode>;

    class StreamInterface : public std::enable_shared_from_this<StreamInterface> {
      protected:
        uint64_t _first_sequence_number = 0;
        std::atomic_uint64_t _next_sequence_number = {0};
        std::atomic_uint64_t _current_size_bytes = {0};

      public:
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
         * Read a record from the stream by its sequence number.
         * Throws exceptions if sequence number does not exist or if the data is corrupted (checksum fails).
         *
         * @param sequence_number the sequence number of the record to read.
         * @return the Record.
         */
        [[nodiscard]] virtual expected<OwnedRecord, StreamError> read(uint64_t sequence_number) const = 0;

        /**
         * Read a record from the stream by its sequence number with an optional hint to the storage engine for where to
         * start looking for the record. Throws exceptions if sequence number does not exist or if the data is corrupted
         * (checksum fails).
         *
         * @param sequence_number the sequence number of the record to read.
         * @param suggested_start an optional hint to the storage engine for where to start looking for the record.
         * @return the Record.
         */
        [[nodiscard]] virtual expected<OwnedRecord, StreamError> read(uint64_t sequence_number,
                                                                      uint64_t suggested_start) const = 0;

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
        std::shared_ptr<FileSystemInterface> file_implementation = {};
        KVOptions kv_options = {
            .filesystem_implementation = file_implementation, .identifier = "kv", .compact_after = 128 * 1024};
    };

    inline CheckpointableOwnedRecord Iterator::operator*() {
        if (auto stream = _stream.lock()) {
            auto record_or = stream->read(sequence_number, _offset);
            if (!record_or) {
                // TODO: No throw
                throw std::runtime_error(record_or.err().msg);
            }
            auto x = std::move(record_or.val());
            timestamp = x.timestamp;
            _offset = x.offset + x.data.size();
            return {std::move(x), [this] { checkpoint(); }};
        }
        throw std::runtime_error("Unable to read from destroyed stream");
    }

    inline void Iterator::checkpoint() const {
        if (auto stream = _stream.lock()) {
            stream->setCheckpoint(_id, sequence_number);
            return;
        }
        throw std::runtime_error("Unable to checkpoint in destroyed stream");
    }

    [[nodiscard]] inline int64_t timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }
} // namespace gg
} // namespace aws