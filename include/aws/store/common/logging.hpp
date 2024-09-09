// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#pragma once
#include <cstdint>
#include <ostream>
#include <string>

namespace aws {
namespace store {
namespace logging {
enum class LogLevel : std::uint8_t {
    Disabled,
    Trace,
    Debug,
    Info,
    Warning,
    Error,
};

static std::string str(const LogLevel level) {
    std::string v{};
    switch (level) {
    case LogLevel::Disabled:
        v = "Disabled";
        break;
    case LogLevel::Trace:
        v = "Trace";
        break;
    case LogLevel::Debug:
        v = "Debug";
        break;
    case LogLevel::Info:
        v = "Info";
        break;
    case LogLevel::Warning:
        v = "Warning";
        break;
    case LogLevel::Error:
        v = "Error";
        break;
    }

    return v;
}

// coverity[autosar_cpp14_a13_2_2_violation] The design of << is such that we should return a reference
inline std::ostream &operator<<(std::ostream &out, const LogLevel level) {
    out << str(level);
    // coverity[misra_cpp_2008_rule_7_5_3_violation] The design of << is such that we should return a reference
    return out;
}

class Logger {
  public:
    LogLevel level{LogLevel::Info};

    Logger() = default;
    virtual ~Logger() = default;

    virtual void log(LogLevel, const std::string &) const = 0;
};
} // namespace logging
} // namespace store
} // namespace aws
