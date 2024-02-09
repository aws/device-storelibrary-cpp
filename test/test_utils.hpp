#pragma once

#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_adapters.hpp>
#include <filesystem>
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

Catch::Generators::GeneratorWrapper<std::string> random(int low, int high) {
    return {Catch::Detail::make_unique<RandomStringGenerator>(low, high, ' ', '~')};
}

Catch::Generators::GeneratorWrapper<std::string> random(int low, int high, char first, char last) {
    return {Catch::Detail::make_unique<RandomStringGenerator>(low, high, first, last)};
}

class TempDir {
    std::filesystem::path _path{std::filesystem::current_path() / random(1, 10, 'a', 'z').get()};

  public:
    TempDir() { std::filesystem::remove_all(_path); }

    ~TempDir() { std::filesystem::remove_all(_path); }

    [[nodiscard]] const std::filesystem::path &path() const { return _path; }
};

} // namespace