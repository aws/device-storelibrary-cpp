#pragma once

#include <string>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    template <class E> struct GenericError {
        E code;
        std::string msg;

        bool ok() const {
            return code == E::NoError;
        }
    };
} // namespace gg
} // namespace aws