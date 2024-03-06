#pragma once

#include "aws/store/common/expected.hpp"
#include "aws/store/common/slices.hpp"
#include "aws/store/common/util.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    enum class FileErrorCode : std::uint8_t {
        NoError,
        InvalidArguments,
        EndOfFile,
        AccessDenied,
        FileDoesNotExist,
        TooManyOpenFiles,
        DiskFull,
        IOError,
        Unknown,
    };

    using FileError = GenericError<FileErrorCode>;

    class FileLike {
      public:
        virtual expected<OwnedSlice, FileError> read(uint32_t begin, uint32_t end) = 0;

        virtual FileError append(BorrowedSlice data) = 0;

        virtual FileError flush() = 0;

        virtual void sync() = 0;

        virtual FileError truncate(uint32_t) = 0;

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
        FileSystemInterface(FileSystemInterface &) = default;
        FileSystemInterface &operator=(FileSystemInterface &) = default;
        FileSystemInterface(FileSystemInterface &&) = default;
        FileSystemInterface &operator=(FileSystemInterface &&) = default;

        virtual ~FileSystemInterface() = default;
    };
} // namespace gg
} // namespace aws