#pragma once

#include <any>
#include <aws/store/common/expected.hpp>
#include <aws/store/filesystem/filesystem.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_adapters.hpp>
#include <filesystem>
#include <functional>
#include <list>
#include <memory> // for unique_ptr
#include <random>
#include <string>
#include <variant>

namespace aws {
namespace gg {
namespace test {
namespace utils {
static void random_string(std::string &s, const size_t len) {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist(0, 25);
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        s.push_back('a' + static_cast<char>(dist(mt)));
    }
}

// TODO consider making Catch generator for filled-up KV store
__attribute__((unused)) static auto generate_key_values(const size_t count) {
    std::vector<std::pair<std::string, std::string>> key_values;
    for (auto i = 0U; i < count; i++) {
        std::string key;
        random_string(key, 512);

        std::string value;
        random_string(value, 1 * 1024 * 1024);

        key_values.emplace_back(key, value);
    }
    return key_values;
}

class RandomStringGenerator : public Catch::Generators::IGenerator<std::string> {
    std::minstd_rand m_rand{std::random_device{}()};
    std::uniform_int_distribution<> m_length_dist;
    std::uniform_int_distribution<> m_value_dist;
    std::string current_string;

  public:
    RandomStringGenerator(int low, int high, char first, char last);

    virtual const std::string &get() const override;
    virtual bool next() override {
        const auto length = m_length_dist(m_rand);
        current_string = std::string{};
        current_string.reserve(static_cast<size_t>(length));
        for (auto i = 0; i < length; i++) {
            current_string += static_cast<char>(m_value_dist(m_rand));
        }
        return true;
    }
};

static __attribute__((unused)) Catch::Generators::GeneratorWrapper<std::string> random(int min_len, int max_len) {
    return {Catch::Detail::make_unique<RandomStringGenerator>(min_len, max_len, ' ', '~')};
}

static Catch::Generators::GeneratorWrapper<std::string> random(int min_len, int max_len, char first, char last) {
    return {Catch::Detail::make_unique<RandomStringGenerator>(min_len, max_len, first, last)};
}

class TempDir {
    std::filesystem::path _path{std::filesystem::current_path() / random(1, 10, 'a', 'z').get()};

  public:
    TempDir() {
        std::filesystem::remove_all(_path);
    }

    ~TempDir() {
        std::filesystem::remove_all(_path);
    }

    const std::filesystem::path &path() const {
        return _path;
    }
};

template <typename> struct RemoveMembership {};
template <typename X, typename Y> struct RemoveMembership<X Y::*> {
    using type = X;
};

struct CallRealMethod {};

class SpyFileLike : public aws::gg::FileLike {
  private:
    std::unique_ptr<aws::gg::FileLike> _real;
    std::list<std::pair<std::string, std::variant<CallRealMethod, std::any>>> _mocks{};

  public:
    using ReadType = std::function<RemoveMembership<decltype(&aws::gg::FileLike::read)>::type>;
    using AppendType = std::function<RemoveMembership<decltype(&aws::gg::FileLike::append)>::type>;
    using FlushType = std::function<RemoveMembership<decltype(&aws::gg::FileLike::flush)>::type>;
    using SyncType = std::function<RemoveMembership<decltype(&aws::gg::FileLike::sync)>::type>;
    using TruncateType = std::function<RemoveMembership<decltype(&aws::gg::FileLike::truncate)>::type>;

    static aws::gg::expected<std::unique_ptr<aws::gg::FileLike>, aws::gg::FileError>
    create(aws::gg::expected<std::unique_ptr<aws::gg::FileLike>, aws::gg::FileError> e) {
        if (e.ok()) {
            return {std::make_unique<SpyFileLike>(std::move(e.val()))};
        }
        return std::move(e.err());
    }

    SpyFileLike(std::unique_ptr<aws::gg::FileLike> f) : _real(std::move(f)) {
    }

    aws::gg::expected<aws::gg::OwnedSlice, aws::gg::FileError> read(const uint32_t begin, const uint32_t end) override {
        if (!_mocks.empty() && _mocks.front().first == "read") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<ReadType>(std::get<std::any>(mock.second));
                return f(begin, end);
            }
        }
        return _real->read(begin, end);
    }

    aws::gg::FileError append(const aws::gg::BorrowedSlice data) override {
        if (!_mocks.empty() && _mocks.front().first == "append") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<AppendType>(std::get<std::any>(mock.second));
                return f(data);
            }
        }
        return _real->append(data);
    }

    aws::gg::FileError flush() override {
        if (!_mocks.empty() && _mocks.front().first == "flush") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<FlushType>(std::get<std::any>(mock.second));
                return f();
            }
        }
        return _real->flush();
    }

    void sync() override {
        if (!_mocks.empty() && _mocks.front().first == "sync") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<SyncType>(std::get<std::any>(mock.second));
                return f();
            }
        }
        _real->sync();
    }

    aws::gg::FileError truncate(const uint32_t s) override {
        if (!_mocks.empty() && _mocks.front().first == "truncate") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<TruncateType>(std::get<std::any>(mock.second));
                return f(s);
            }
        }
        return _real->truncate(s);
    }

    template <typename ret, typename... args> auto when(const std::string &method, std::function<ret(args...)> f) {
        std::ignore = _mocks.emplace_back(method, f);
        return this;
    }

    auto when(const std::string &method, CallRealMethod f) {
        std::ignore = _mocks.emplace_back(method, f);
        return this;
    }
};

class SpyFileSystem : public aws::gg::FileSystemInterface {
  private:
    std::list<std::pair<std::string, std::variant<CallRealMethod, std::any>>> _mocks{};

  public:
    std::shared_ptr<aws::gg::FileSystemInterface> real;

    using OpenType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::open)>::type>;
    using ExistsType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::exists)>::type>;
    using RenameType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::rename)>::type>;
    using RemoveType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::remove)>::type>;
    using ListType = std::function<RemoveMembership<decltype(&aws::gg::FileSystemInterface::list)>::type>;

    SpyFileSystem(std::shared_ptr<aws::gg::FileSystemInterface> real) : real(std::move(real)) {
    }

    aws::gg::expected<std::unique_ptr<aws::gg::FileLike>, aws::gg::FileError>
    open(const std::string &identifier) override {
        if (!_mocks.empty() && _mocks.front().first == "open") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<OpenType>(std::get<std::any>(mock.second));
                return f(identifier);
            }
        }
        return SpyFileLike::create(real->open(identifier));
    };

    bool exists(const std::string &identifier) override {
        if (!_mocks.empty() && _mocks.front().first == "exists") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<ExistsType>(std::get<std::any>(mock.second));
                return f(identifier);
            }
        }
        return real->exists(identifier);
    };

    aws::gg::FileError rename(const std::string &old_id, const std::string &new_id) override {
        if (!_mocks.empty() && _mocks.front().first == "rename") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<RenameType>(std::get<std::any>(mock.second));
                return f(old_id, new_id);
            }
        }
        return real->rename(old_id, new_id);
    };

    aws::gg::FileError remove(const std::string &id) override {
        if (!_mocks.empty() && _mocks.front().first == "remove") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<RemoveType>(std::get<std::any>(mock.second));
                return f(id);
            }
        }
        return real->remove(id);
    };

    aws::gg::expected<std::vector<std::string>, aws::gg::FileError> list() override {
        if (!_mocks.empty() && _mocks.front().first == "list") {
            const auto mock = _mocks.front();
            _mocks.pop_front();
            if (std::holds_alternative<std::any>(mock.second)) {
                const auto f = std::any_cast<ListType>(std::get<std::any>(mock.second));
                return f();
            }
        }
        return real->list();
    };

    template <typename ret, typename... args> auto when(const std::string &method, std::function<ret(args...)> f) {
        std::ignore = _mocks.emplace_back(method, f);
        return this;
    }

    auto when(const std::string &method, CallRealMethod f) {
        std::ignore = _mocks.emplace_back(method, f);
        return this;
    }
};
} // namespace utils
} // namespace test
} // namespace gg
} // namespace aws