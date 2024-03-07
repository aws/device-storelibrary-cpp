#pragma once

#include <string>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    // coverity[misra_cpp_2008_rule_3_2_2_violation] false positive. Templates do not violate ODR
    template <class E> struct GenericError {
        E code;
        std::string msg;

        bool ok() const {
            return code == E::NoError;
        }
    };
} // namespace gg
} // namespace aws