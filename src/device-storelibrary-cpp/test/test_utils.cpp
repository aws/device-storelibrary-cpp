#include "test_utils.hpp"

namespace aws {
namespace store {
namespace test {
namespace utils {
RandomStringGenerator::RandomStringGenerator(const int low, const int high, const char first, const char last)
    : m_length_dist(low, high), m_value_dist(first, last) {
    static_cast<void>(next());
}

const std::string &RandomStringGenerator::get() const {
    return current_string;
}
} // namespace utils
} // namespace test
} // namespace store
} // namespace aws
