#pragma once
#include "db.hpp"
#include <filesystem>

namespace aws {
namespace gg __attribute__((visibility("default"))) {

    class PosixFileLike : public FileLike {
        std::filesystem::path _path;
        FILE *_f = nullptr;

      public:
        explicit PosixFileLike(std::filesystem::path &&path) : _path(std::move(path)) {
            _f = std::fopen(_path.c_str(), "ab+");
            if (!_f) {
                throw std::runtime_error(std::string{"Cannot open file "} + std::strerror(errno));
            }
        };
        PosixFileLike(PosixFileLike &&) = default;
        PosixFileLike(PosixFileLike &) = delete;
        PosixFileLike operator=(PosixFileLike &) = delete;
        PosixFileLike &operator=(PosixFileLike &&) = default;

        ~PosixFileLike() override {
            if (_f) {
                std::fclose(_f);
            }
        }

        OwnedSlice read(size_t begin, size_t end) override {
            if (end <= begin) {
                throw std::runtime_error("End must be after the beginning");
            }
            clearerr(_f);
            auto d = OwnedSlice{(end - begin)};
            if (std::fseek(_f, begin, SEEK_SET) != 0) {
                throw std::runtime_error(std::strerror(errno));
            }
            if (std::fread((void *)d.data(), d.size(), 1, _f) != 1) {
                if (feof(_f) != 0) {
                    throw std::runtime_error("EOF");
                } else {
                    throw std::runtime_error(std::strerror(errno));
                }
            }
            return d;
        };

        void append(BorrowedSlice data) override {
            if (fwrite(data.data(), data.size(), 1, _f) != 1) {
                throw std::runtime_error(std::strerror(errno));
            }
        };

        void flush() override { fflush(_f); }
    };

    class PosixFileSystem : public FileSystemInterface {
      private:
        std::filesystem::path _base_path;

      public:
        explicit PosixFileSystem(std::filesystem::path &&base_path) : _base_path(std::move(base_path)) {
            std::filesystem::create_directories(_base_path);
        };

        std::unique_ptr<FileLike> open(std::string identifier) override {
            return std::make_unique<PosixFileLike>(_base_path / identifier);
        };

        bool exists(std::string identifier) override { return std::filesystem::exists(_base_path / identifier); };

        void rename(std::string old_id, std::string new_id) override {
            std::filesystem::rename(_base_path / old_id, _base_path / new_id);
        };

        void remove(std::string id) override { std::filesystem::remove(_base_path / id); };

        std::vector<std::string> list() override {
            std::vector<std::string> output;
            for (const auto &entry : std::filesystem::directory_iterator(_base_path)) {
                output.emplace_back(entry.path().filename().string());
            }
            return output;
        };
    };
} // namespace gg
} // namespace aws