#pragma once

#include "expected.hpp"
#include <atomic>
#include <chrono>
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
    class BorrowedSlice {
      private:
        const uint8_t *_data;
        const size_t _size;

      public:
        BorrowedSlice(const uint8_t *data, const size_t size) : _data(data), _size(size){};

        [[nodiscard]] const uint8_t *data() const { return _data; };

        [[nodiscard]] size_t size() const { return _size; };

        [[nodiscard]] std::string string() const { return {reinterpret_cast<const char *>(_data), _size}; };
    };

    class OwnedSlice : private std::unique_ptr<uint8_t[]> {
      private:
        size_t _size;

      public:
        OwnedSlice() = default;
        explicit OwnedSlice(BorrowedSlice b) : _size(b.size()) {
            std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
            memcpy(mem.get(), b.data(), b.size());
            swap(mem);
        }

        explicit OwnedSlice(size_t size) : _size(size) {
            std::unique_ptr<uint8_t[]> mem{new (std::nothrow) uint8_t[_size]};
            swap(mem);
        }

        OwnedSlice(uint8_t d[], const size_t size) : _size(size) { reset(d); };

        OwnedSlice(OwnedSlice &&) = default;
        OwnedSlice(OwnedSlice &) = delete;
        OwnedSlice operator=(OwnedSlice &) = delete;
        OwnedSlice &operator=(OwnedSlice &&) = default;

        ~OwnedSlice() = default;

        [[nodiscard]] uint8_t *data() const { return get(); };

        [[nodiscard]] size_t size() const { return _size; };

        [[nodiscard]] std::string string() const { return {reinterpret_cast<const char *>(get()), _size}; };
    };

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

    enum class FileErrorCode : std::uint8_t {
        NoError,
        InvalidArguments,
        EndOfFile,
        InsufficientPermissions,
        FileDoesNotExist,
        Unknown,
    };

    enum class DBErrorCode : std::uint8_t {
        NoError,
        RecordNotFound,
        RecordDataCorrupted,
        HeaderDataCorrupted,
        RecordTooLarge,
        ReadError,
        WriteError,
        Unknown,
    };

    template <class E> struct GenericError {
        E code;
        std::string msg;
    };

    using FileError = GenericError<FileErrorCode>;

    class FileLike {
      public:
        virtual expected<OwnedSlice, FileError> read(size_t begin, size_t end) = 0;

        virtual FileError append(BorrowedSlice data) = 0;

        virtual void flush() = 0;

        FileLike(FileLike &) = delete;

        FileLike &operator=(FileLike &) = delete;

        FileLike(FileLike &&) = default;

        FileLike &operator=(FileLike &&) = default;

        FileLike() = default;

        virtual ~FileLike() = default;
    };

    class FileSystemInterface {
      public:
        virtual expected<std::unique_ptr<FileLike>, FileError> open(const std::string &identifier) = 0;

        virtual bool exists(const std::string &identifier) = 0;

        virtual FileError rename(const std::string &old_id, const std::string &new_id) = 0;

        virtual FileError remove(const std::string &) = 0;

        virtual expected<std::vector<std::string>, FileError> list() = 0;

        FileSystemInterface() = default;

        virtual ~FileSystemInterface() = default;
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
        char _id;
        uint64_t _offset = 0;

        void checkpoint() const;

      public:
        explicit Iterator(std::weak_ptr<StreamInterface> s, char id, uint64_t seq)
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

    using DBError = GenericError<DBErrorCode>;

    class StreamInterface : public std::enable_shared_from_this<StreamInterface> {
      protected:
        uint64_t _first_sequence_number = 0;
        std::atomic_uint64_t _next_sequence_number = {0};
        std::atomic_uint64_t _current_size_bytes = {0};

      public:
        StreamInterface(StreamInterface &) = delete;

        /**
         * Append data into the DB.
         *
         * @return the sequence number of the record appended.
         */
        virtual expected<uint64_t, DBError> append(BorrowedSlice) = 0;

        /**
         * Append data into the DB.
         *
         * @return the sequence number of the record appended.
         */
        virtual expected<uint64_t, DBError> append(OwnedSlice &&) = 0;

        /**
         * Read a record from the DB by its sequence number.
         * Throws exceptions if sequence number does not exist or if the data is corrupted (checksum fails).
         *
         * @param sequence_number the sequence number of the record to read.
         * @return the Record.
         */
        [[nodiscard]] virtual expected<OwnedRecord, DBError> read(uint64_t sequence_number) const = 0;

        /**
         * Read a record from the DB by its sequence number with an optional hint to the storage engine for where to
         * start looking for the record. Throws exceptions if sequence number does not exist or if the data is corrupted
         * (checksum fails).
         *
         * @param sequence_number the sequence number of the record to read.
         * @param suggested_start an optional hint to the storage engine for where to start looking for the record.
         * @return the Record.
         */
        [[nodiscard]] virtual expected<OwnedRecord, DBError> read(uint64_t sequence_number,
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
        [[nodiscard]] virtual Iterator openOrCreateIterator(char identifier, IteratorOptions) = 0;

        /**
         * Delete the persisted iterator if present. Nop if no iterator exists with the given identifier.
         *
         * @param identifier identifier of the iterator to delete.
         */
        virtual void deleteIterator(char identifier) = 0;

        /**
         * Persist a checkpoint for the iterator identified at the given sequence number.
         *
         * @param identifier identifier of the iterator to associate with this checkpoint.
         * @param sequence_number sequence number of the record. When opening an existing iterator it will start from
         * this record.
         */
        virtual void setCheckpoint(char identifier, uint64_t sequence_number) = 0;

        StreamInterface() = default;

        virtual ~StreamInterface() = default;
    };

    struct StreamOptions {
        size_t minimum_segment_size_bytes = 16 * 1024 * 1024; // 16MB minimum segment size before making a new segment
        size_t maximum_db_size_bytes = 128 * 1024 * 1024;     // 128MB max db size
        std::shared_ptr<FileSystemInterface> file_implementation = {};
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