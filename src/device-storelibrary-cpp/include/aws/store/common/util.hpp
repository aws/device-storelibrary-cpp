#pragma once

#include <string>

namespace aws {
namespace store {
namespace common {
// coverity[misra_cpp_2008_rule_3_2_2_violation] false positive. Templates do not violate ODR
template <class E> struct GenericError {
    E code;
    std::string msg;

    bool ok() const {
        return code == E::NoError;
    }
};
} // namespace common
} // namespace store
} // namespace aws
