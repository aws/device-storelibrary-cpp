#pragma once
#include "common/expected.hpp"
#include "filesystem.hpp"
#include <filesystem>
#include <mutex>
#include <unistd.h>
#include <utility>

namespace aws {
namespace gg __attribute__((visibility("default"))) {

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
                // TODO: error code mapping
                return FileError{FileErrorCode::Unknown, std::strerror(errno)};
            }
            return FileError{FileErrorCode::NoError, {}};
        }

        expected<OwnedSlice, FileError> read(size_t begin, size_t end) override {
            if (end < begin) {
                return FileError{FileErrorCode::InvalidArguments, "End must be after the beginning"};
            } else if (end == begin) {
                return FileError{FileErrorCode::InvalidArguments, "Beginning and end should not be equal"};
            }

            std::lock_guard<std::mutex> lock(_read_lock);
            clearerr(_f);
            auto d = OwnedSlice{(end - begin)};
            if (std::fseek(_f, begin, SEEK_SET) != 0) {
                // TODO: error code mapping
                return FileError{FileErrorCode::Unknown, std::strerror(errno)};
            }
            if (std::fread((void *)d.data(), d.size(), 1, _f) != 1) {
                if (feof(_f) != 0) {
                    return {FileError{FileErrorCode::EndOfFile, {}}};
                } else {
                    // TODO: error code mapping
                    return FileError{FileErrorCode::Unknown, std::strerror(errno)};
                }
            }
            return d;
        };

        FileError append(BorrowedSlice data) override {
            if (fwrite(data.data(), data.size(), 1, _f) != 1) {
                // TODO: error code mapping
                return {FileErrorCode::Unknown, std::strerror(errno)};
            }
            return {FileErrorCode::NoError, {}};
        };

        void flush() override { fflush(_f); }

        FileError truncate(size_t max) override {
            if (ftruncate(fileno(_f), max) != 0) {
                // TODO: error code mapping
                return {FileErrorCode::Unknown, std::strerror(errno)};
            }
            return {FileErrorCode::NoError, {}};
        }
    };

    class PosixFileSystem : public FileSystemInterface {
      private:
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
                return {std::move(res)};
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
            // TODO: error code mapping
            return {FileErrorCode::Unknown, ec.message()};
        };

        FileError remove(const std::string &id) override {
            std::error_code ec;
            std::filesystem::remove(_base_path / id, ec);
            if (!ec) {
                return {FileErrorCode::NoError, {}};
            }
            // TODO: error code mapping
            return {FileErrorCode::Unknown, ec.message()};
        };

        expected<std::vector<std::string>, FileError> list() override {
            std::vector<std::string> output;
            for (const auto &entry : std::filesystem::directory_iterator(_base_path)) {
                output.emplace_back(entry.path().filename().string());
            }
            return output;
        };
    };
} // namespace gg
} // namespace aws