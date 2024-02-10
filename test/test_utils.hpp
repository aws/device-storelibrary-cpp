#pragma once

#include "common/expected.hpp"
#include "filesystem/filesystem.hpp"
#include <any>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_adapters.hpp>
#include <filesystem>
#include <list>
#include <memory>
#include <random>
#include <string>

namespace {

class RandomStringGenerator : public Catch::Generators::IGenerator<std::string> {
    std::minstd_rand m_rand{std::random_device{}()};
    std::uniform_int_distribution<> m_length_dist;
    std::uniform_int_distribution<> m_value_dist;
    std::string current_string;

  public:
    RandomStringGenerator(int low, int high, char first, char last)
        : m_length_dist(low, high), m_value_dist(first, last) {
        static_cast<void>(next());
    }

    std::string const &get() const override;
    bool next() override {
        auto length = m_length_dist(m_rand);
        current_string = std::string{};
        current_string.reserve(length);
        for (size_t i = 0; i < length; i++) {
            current_string += static_cast<char>(m_value_dist(m_rand));
        }
        return true;
    }
};

std::string const &RandomStringGenerator::get() const { return current_string; }

Catch::Generators::GeneratorWrapper<std::string> random(int min_len, int max_len) {
    return {Catch::Detail::make_unique<RandomStringGenerator>(min_len, max_len, ' ', '~')};
}

Catch::Generators::GeneratorWrapper<std::string> random(int min_len, int max_len, char first, char last) {
    return {Catch::Detail::make_unique<RandomStringGenerator>(min_len, max_len, first, last)};
}

class TempDir {
    std::filesystem::path _path{std::filesystem::current_path() / random(1, 10, 'a', 'z').get()};

  public:
    TempDir() { std::filesystem::remove_all(_path); }

    ~TempDir() { std::filesystem::remove_all(_path); }

    [[nodiscard]] const std::filesystem::path &path() const { return _path; }
};

class SpyFileLike : public aws::gg::FileLike {
  private:
    std::unique_ptr<aws::gg::FileLike> _real;

  public:
    static aws::gg::expected<std::unique_ptr<aws::gg::FileLike>, aws::gg::FileError>
    create(aws::gg::expected<std::unique_ptr<aws::gg::FileLike>, aws::gg::FileError> e) {
        if (e) {
            return {std::make_unique<SpyFileLike>(std::move(e.val()))};
        }
        return std::move(e.err());
    }

    SpyFileLike(std::unique_ptr<aws::gg::FileLike> f) : _real(std::move(f)) {}

    aws::gg::expected<aws::gg::OwnedSlice, aws::gg::FileError> read(size_t begin, size_t end) override {
        return _real->read(begin, end);
    }

    aws::gg::FileError append(aws::gg::BorrowedSlice data) override { return _real->append(data); }

    void flush() override { return _real->flush(); }

    aws::gg::FileError truncate(size_t s) override { return _real->truncate(s); }
};

template <typename> struct RemoveMembership {};
template <typename X, typename Y> struct RemoveMembership<X Y::*> {
    using type = X;
};

class SpyFileSystem : public aws::gg::FileSystemInterface {
  private:
    std::shared_ptr<aws::gg::FileSystemInterface> _real;
    std::list<std::pair<std::string, std::any>> _mocks{};

  public:
    using OpenType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::open)>::type>;
    using ExistsType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::exists)>::type>;
    using RenameType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::rename)>::type>;
    using RemoveType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::remove)>::type>;
    using ListType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::list)>::type>;

    SpyFileSystem(std::shared_ptr<aws::gg::FileSystemInterface> real) : _real(std::move(real)) {}

    aws::gg::expected<std::unique_ptr<aws::gg::FileLike>, aws::gg::FileError> open(const std::string &identifier) {
        if (!_mocks.empty() && _mocks.front().first == "open") {
            auto mock = _mocks.front();
            _mocks.pop_front();
            auto f = std::any_cast<OpenType>(mock.second);
            return f(identifier);
        }
        return SpyFileLike::create(_real->open(identifier));
    };

    bool exists(const std::string &identifier) {
        if (!_mocks.empty() && _mocks.front().first == "exists") {
            auto mock = _mocks.front();
            _mocks.pop_front();
            auto f = std::any_cast<ExistsType>(mock.second);
            return f(identifier);
        }
        return _real->exists(identifier);
    };

    aws::gg::FileError rename(const std::string &old_id, const std::string &new_id) {
        if (!_mocks.empty() && _mocks.front().first == "rename") {
            auto mock = _mocks.front();
            _mocks.pop_front();
            auto f = std::any_cast<RenameType>(mock.second);
            return f(old_id, new_id);
        }
        return _real->rename(old_id, new_id);
    };

    aws::gg::FileError remove(const std::string &id) {
        if (!_mocks.empty() && _mocks.front().first == "remove") {
            auto mock = _mocks.front();
            _mocks.pop_front();
            auto f = std::any_cast<RemoveType>(mock.second);
            return f(id);
        }
        return _real->remove(id);
    };

    aws::gg::expected<std::vector<std::string>, aws::gg::FileError> list() {
        if (!_mocks.empty() && _mocks.front().first == "list") {
            auto mock = _mocks.front();
            _mocks.pop_front();
            auto f = std::any_cast<ListType>(mock.second);
            return f();
        }
        return _real->list();
    };

    template <typename ret, typename... args> auto when(const std::string &method, std::function<ret(args...)> f) {
        _mocks.push_back({method, f});
        return this;
    }
};

} // namespace