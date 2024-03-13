#pragma once
#include <aws/store/common/expected.hpp>
#include <aws/store/filesystem/filesystem.hpp>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <unistd.h>

namespace aws {
namespace store {
namespace filesystem {
static void sync(int fileno) {
    // Only sync data if available on this OS. Otherwise, just fsync.
#if _POSIX_SYNCHRONIZED_IO > 0
    std::ignore = fdatasync(fileno);
#else
    std::ignore = fsync(fileno);
#endif
}

static FileError errnoToFileError(const int err, const std::string &str = {}) {
    switch (err) {
    case EACCES:
        return FileError{FileErrorCode::AccessDenied, str + " Access denied"};
    case EDQUOT:
        return FileError{FileErrorCode::DiskFull, str + " User inode/disk block quota exhausted"};
    case EINVAL:
        return FileError{FileErrorCode::InvalidArguments, str + " Unknown invalid arguments"};
    case EISDIR:
        return FileError{FileErrorCode::InvalidArguments,
                         str + " Path cannot be opened for writing because it is a directory"};
    case ELOOP:
        return FileError{FileErrorCode::InvalidArguments, str + " Too many symbolic links"};
    case EMFILE: // fallthrough
    case ENFILE:
        return FileError{FileErrorCode::TooManyOpenFiles, str + " Too many open files. Consider raising limits."};
    case ENOENT:
        return FileError{FileErrorCode::FileDoesNotExist, str + " Path does not exist"};
    case EFBIG:
        return FileError{FileErrorCode::InvalidArguments, str + " File is too large"};
    case EIO:
        return FileError{FileErrorCode::IOError, str + " Unknown IO error"};
    case ENOSPC:
        return FileError{FileErrorCode::DiskFull, str + " Disk full"};
    default:
        return FileError{FileErrorCode::Unknown, str + " Unknown error code: " + std::to_string(err)};
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

    virtual ~PosixFileLike() override {
        if (_f != nullptr) {
            std::ignore = std::fclose(_f);
        }
    }

    virtual FileError open() noexcept {
        _f = std::fopen(_path.c_str(), "ab+");
        if (!_f) {
            return errnoToFileError(errno);
        }
        return FileError{FileErrorCode::NoError, {}};
    }

    virtual common::Expected<common::OwnedSlice, FileError> read(const uint32_t begin, const uint32_t end) override {
        if (end < begin) {
            return FileError{FileErrorCode::InvalidArguments, "End must be after the beginning"};
        }
        if (end == begin) {
            return common::OwnedSlice{0U};
        }

        std::lock_guard lock{_read_lock};
        clearerr(_f);
        auto d = common::OwnedSlice{(end - begin)};
        if (std::fseek(_f, static_cast<off_t>(begin), SEEK_SET) != 0) {
            return errnoToFileError(errno);
        }
        if (std::fread(d.data(), d.size(), 1U, _f) != 1U) {
            if (feof(_f) != 0) {
                return {FileError{FileErrorCode::EndOfFile, {}}};
            }
            return errnoToFileError(errno);
        }
        return d;
    };

    virtual FileError append(const common::BorrowedSlice data) override {
        clearerr(_f);
        if (fwrite(data.data(), data.size(), 1U, _f) != 1U) {
            return errnoToFileError(errno);
        }
        return {FileErrorCode::NoError, {}};
    };

    virtual FileError flush() override {
        if (fflush(_f) == 0) {
            return FileError{FileErrorCode::NoError, {}};
        }
        return errnoToFileError(errno);
    }

    virtual void sync() override {
        aws::store::filesystem::sync(fileno(_f));
    }

    virtual FileError truncate(const uint32_t max) override {
        // Flush buffers before truncating since truncation is operating on the FD directly rather than the file
        // stream
        std::ignore = flush();
        if (ftruncate(fileno(_f), static_cast<off_t>(max)) != 0) {
            return errnoToFileError(errno);
        }
        return flush();
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

    virtual ~PosixUnbufferedFileLike() override {
        if (_f > 0) {
            std::ignore = ::close(_f);
        }
    }

    virtual FileError open() noexcept {
        // open file or create with permissions 660
        _f = ::open(_path.c_str(), O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (_f <= 0) {
            return errnoToFileError(errno);
        }
        return FileError{FileErrorCode::NoError, {}};
    }

    virtual common::Expected<common::OwnedSlice, FileError> read(const uint32_t begin, const uint32_t end) override {
        if (end < begin) {
            return FileError{FileErrorCode::InvalidArguments, "End must be after the beginning"};
        }
        if (end == begin) {
            return common::OwnedSlice{0U};
        }

        std::lock_guard lock{_read_lock};
        auto d = common::OwnedSlice{(end - begin)};

        if (lseek(_f, static_cast<off_t>(begin), SEEK_SET) < 0) {
            return errnoToFileError(errno);
        }

        auto *read_pointer = static_cast<uint8_t *>(d.data());
        uint32_t read_remaining = d.size();

        while (read_remaining > 0U) {
            const auto did_read = ::read(_f, read_pointer, read_remaining);

            if (did_read == 0) {
                return {FileError{FileErrorCode::EndOfFile, {}}};
            }
            if (did_read < 0) {
                return errnoToFileError(errno);
            }

            read_remaining -= static_cast<uint32_t>(did_read);
            read_pointer += did_read;
        }
        return d;
    };

    virtual FileError append(const common::BorrowedSlice data) override {
        const auto *write_pointer = static_cast<const uint8_t *>(data.data());
        uint32_t write_remaining = data.size();

        while (write_remaining > 0U) {
            const auto did_write = ::write(_f, write_pointer, write_remaining);
            if (did_write <= 0) {
                return errnoToFileError(errno);
            }

            write_remaining -= static_cast<uint32_t>(did_write);
            write_pointer += did_write;
        }

        return FileError{FileErrorCode::NoError, {}};
    };

    virtual FileError flush() override {
        return FileError{FileErrorCode::NoError, {}};
    }

    virtual void sync() override {
        aws::store::filesystem::sync(_f);
    }

    virtual FileError truncate(const uint32_t max) override {
        if (ftruncate(_f, static_cast<off_t>(max)) != 0) {
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

    virtual common::Expected<std::unique_ptr<FileLike>, FileError> open(const std::string &identifier) override {
        auto f = std::make_unique<PosixFileLike>(_base_path / identifier);
        auto res = f->open();
        if (res.ok()) {
            return {std::move(f)};
        }
        return res;
    };

    virtual bool exists(const std::string &identifier) override {
        return std::filesystem::exists(_base_path / identifier);
    };

    virtual FileError rename(const std::string &old_id, const std::string &new_id) override {
        std::error_code ec;
        std::filesystem::rename(_base_path / old_id, _base_path / new_id, ec);
        if (!ec) {
            return {FileErrorCode::NoError, {}};
        }
        return errnoToFileError(ec.value(), ec.message());
    };

    virtual FileError remove(const std::string &id) override {
        std::error_code ec;
        std::ignore = std::filesystem::remove(_base_path / id, ec);
        if (!ec) {
            return {FileErrorCode::NoError, {}};
        }
        return errnoToFileError(ec.value(), ec.message());
    };

    virtual common::Expected<std::vector<std::string>, FileError> list() override {
        std::vector<std::string> output;
        for (const auto &entry : std::filesystem::directory_iterator(_base_path)) {
            std::ignore = output.emplace_back(entry.path().filename().string());
        }
        return output;
    };
};

class PosixUnbufferedFileSystem : public PosixFileSystem {
  public:
    explicit PosixUnbufferedFileSystem(std::filesystem::path base_path) : PosixFileSystem(std::move(base_path)){};

    virtual common::Expected<std::unique_ptr<FileLike>, FileError> open(const std::string &identifier) override {
        auto f = std::make_unique<PosixUnbufferedFileLike>(_base_path / identifier);
        auto res = f->open();
        if (res.ok()) {
            return {std::move(f)};
        }
        return res;
    };
};
} // namespace filesystem
} // namespace store
} // namespace aws
