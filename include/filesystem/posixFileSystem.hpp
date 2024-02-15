#pragma once
#include "common/expected.hpp"
#include "filesystem.hpp"
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <unistd.h>
#include <utility>

namespace aws {
namespace gg __attribute__((visibility("default"))) {

    static inline void sync(int fileno) {
        // Only sync data if available on this OS. Otherwise, just fsync.
#if _POSIX_SYNCHRONIZED_IO > 0
        fdatasync(fileno);
#else
        fsync(fileno);
#endif
    }

    static FileError errnoToFileError(int err, const std::string& str = {}) {
        using namespace std::string_literals;
        switch (err) {
            case EACCES:
                return FileError{FileErrorCode::AccessDenied, str + " Access denied"s};
            case EDQUOT:
                return FileError{FileErrorCode::DiskFull, str + " User inode/disk block quota exhausted"s};
            case EINVAL:
                return FileError{FileErrorCode::InvalidArguments, str + " Unknown invalid arguments"s};
            case EISDIR:
                return FileError{FileErrorCode::InvalidArguments, str + " Path cannot be opened for writing because it is a directory"s};
            case ELOOP:
                return FileError{FileErrorCode::InvalidArguments, str + " Too many symbolic links"s};
            case EMFILE:
                [[fallthrough]];
            case ENFILE:
                return FileError{FileErrorCode::TooManyOpenFiles, str + " Too many open files. Consider raising limits."s};
            case ENOENT:
                return FileError{FileErrorCode::FileDoesNotExist, str + " Path does not exist"s};
            case EFBIG:
                return FileError{FileErrorCode::InvalidArguments, str + " File is too large"s};
            case EIO:
                return FileError{FileErrorCode::IOError, str + " Unknown IO error"s};
            case ENOSPC:
                return FileError{FileErrorCode::DiskFull, str + " Disk full"s};
            default:
                return FileError{FileErrorCode::Unknown, str + " Unknown error code: "s + std::to_string(err)};
        }
    }

    class PosixFileLike : public FileLike {
        std::mutex _read_lock{};
        std::filesystem::path _path;
        FILE *_f = nullptr;

      public:
        explicit PosixFileLike(std::filesystem::path &&path) : _path(std::move(path)){};
        PosixFileLike(PosixFileLike &&) = delete;
        PosixFileLike(PosixFileLike &) = delete;
        PosixFileLike &operator=(PosixFileLike &) = delete;
        PosixFileLike &operator=(PosixFileLike &&) = delete;

        ~PosixFileLike() override {
            if (_f) {
                std::fclose(_f);
            }
        }

        FileError open() noexcept {
            _f = std::fopen(_path.c_str(), "ab+");
            if (!_f) {
                return errnoToFileError(errno);
            }
            return FileError{FileErrorCode::NoError, {}};
        }

        expected<OwnedSlice, FileError> read(uint32_t begin, uint32_t end) override {
            if (end < begin) {
                return FileError{FileErrorCode::InvalidArguments, "End must be after the beginning"};
            } else if (end == begin) {
                return OwnedSlice{0};
            }

            std::lock_guard<std::mutex> lock(_read_lock);
            clearerr(_f);
            auto d = OwnedSlice{(end - begin)};
            if (std::fseek(_f, begin, SEEK_SET) != 0) {
                return errnoToFileError(errno);
            }
            if (std::fread((void *)d.data(), d.size(), 1, _f) != 1) {
                if (feof(_f) != 0) {
                    return {FileError{FileErrorCode::EndOfFile, {}}};
                } else {
                    return errnoToFileError(errno);
                }
            }
            return d;
        };

        FileError append(BorrowedSlice data) override {
            clearerr(_f);
            if (fwrite(data.data(), data.size(), 1, _f) != 1) {
                return errnoToFileError(errno);
            }
            return {FileErrorCode::NoError, {}};
        };

        FileError flush() override {
            if (fflush(_f) == 0) {
                return FileError{FileErrorCode::NoError, {}};
            }
            return errnoToFileError(errno);
        }

        void sync() override { aws::gg::sync(fileno(_f)); }

        FileError truncate(uint32_t max) override {
            if (ftruncate(fileno(_f), max) != 0) {
                return errnoToFileError(errno);
            }
            return {FileErrorCode::NoError, {}};
        }
    };

    class PosixUnbufferedFileLike : public FileLike {
        int _f{0};
        std::mutex _read_lock{};
        std::filesystem::path _path;

      public:
        explicit PosixUnbufferedFileLike(std::filesystem::path &&path) : _path(std::move(path)){};
        PosixUnbufferedFileLike(PosixUnbufferedFileLike &&) = delete;
        PosixUnbufferedFileLike(PosixUnbufferedFileLike &) = delete;
        PosixUnbufferedFileLike &operator=(PosixUnbufferedFileLike &) = delete;
        PosixUnbufferedFileLike &operator=(PosixUnbufferedFileLike &&) = delete;

        ~PosixUnbufferedFileLike() override {
            if (_f) {
                ::close(_f);
            }
        }

        FileError open() noexcept {
            // open file or create with permissions 660
            _f = ::open(_path.c_str(), O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
            if (_f <= 0) {
                return errnoToFileError(errno);
            }
            return FileError{FileErrorCode::NoError, {}};
        }

        expected<OwnedSlice, FileError> read(uint32_t begin, uint32_t end) override {
            if (end < begin) {
                return FileError{FileErrorCode::InvalidArguments, "End must be after the beginning"};
            } else if (end == begin) {
                return OwnedSlice{0};
            }

            std::lock_guard<std::mutex> lock(_read_lock);
            auto d = OwnedSlice{(end - begin)};

            if (lseek(_f, begin, SEEK_SET) < 0) {
                return errnoToFileError(errno);
            }

            uint8_t *read_pointer = d.data();
            uint32_t read_remaining = d.size();

            while (read_remaining > 0) {
                int did_read = ::read(_f, read_pointer, read_remaining);

                if (did_read == 0) {
                    return {FileError{FileErrorCode::EndOfFile, {}}};
                }
                if (did_read < 0) {
                    return errnoToFileError(errno);
                }

                read_remaining -= did_read;
                read_pointer += did_read;
            }
            return d;
        };

        FileError append(BorrowedSlice data) override {
            const uint8_t *write_pointer = data.data();
            uint32_t write_remaining = data.size();

            while (write_remaining > 0) {
                int did_write = ::write(_f, write_pointer, write_remaining);
                if (did_write <= 0) {
                    return errnoToFileError(errno);
                }

                write_remaining -= did_write;
                write_pointer += did_write;
            }

            return FileError{FileErrorCode::NoError, {}};
        };

        FileError flush() override { return FileError{FileErrorCode::NoError, {}}; }

        void sync() override { aws::gg::sync(_f); }

        FileError truncate(uint32_t max) override {
            if (ftruncate(_f, max) != 0) {
                return errnoToFileError(errno);
            }
            return {FileErrorCode::NoError, {}};
        }
    };

    class PosixFileSystem : public FileSystemInterface {
      protected:
        std::filesystem::path _base_path;

      public:
        explicit PosixFileSystem(std::filesystem::path base_path) : _base_path(std::move(base_path)) {
            std::filesystem::create_directories(_base_path);
        };

        expected<std::unique_ptr<FileLike>, FileError> open(const std::string &identifier) override {
            auto f = std::make_unique<PosixFileLike>(_base_path / identifier);
            auto res = f->open();
            if (res.code == FileErrorCode::NoError) {
                return {std::move(f)};
            } else {
                return res;
            }
        };

        bool exists(const std::string &identifier) override {
            return std::filesystem::exists(_base_path / identifier);
        };

        FileError rename(const std::string &old_id, const std::string &new_id) override {
            std::error_code ec;
            std::filesystem::rename(_base_path / old_id, _base_path / new_id, ec);
            if (!ec) {
                return {FileErrorCode::NoError, {}};
            }
            return errnoToFileError(ec.value(), ec.message());
        };

        FileError remove(const std::string &id) override {
            std::error_code ec;
            std::filesystem::remove(_base_path / id, ec);
            if (!ec) {
                return {FileErrorCode::NoError, {}};
            }
            return errnoToFileError(ec.value(), ec.message());
        };

        expected<std::vector<std::string>, FileError> list() override {
            std::vector<std::string> output;
            for (const auto &entry : std::filesystem::directory_iterator(_base_path)) {
                output.emplace_back(entry.path().filename().string());
            }
            return output;
        };
    };

    class PosixUnbufferedFileSystem : public PosixFileSystem {
      public:
        explicit PosixUnbufferedFileSystem(std::filesystem::path base_path) : PosixFileSystem(std::move(base_path)){};

        expected<std::unique_ptr<FileLike>, FileError> open(const std::string &identifier) override {
            auto f = std::make_unique<PosixUnbufferedFileLike>(_base_path / identifier);
            auto res = f->open();
            if (res.code == FileErrorCode::NoError) {
                return {std::move(f)};
            } else {
                return res;
            }
        };
    };
} // namespace gg
} // namespace aws