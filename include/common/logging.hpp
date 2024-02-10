#pragma once
#include <cstdint>
#include <ostream>
#include <string>

namespace aws {
namespace gg {
namespace logging __attribute__((visibility("default"))) {
    enum class LogLevel : std::uint8_t {
        Disabled,
        Trace,
        Debug,
        Info,
        Warning,
        Error,
    };

    static inline std::string str(LogLevel level) {
        using namespace std::string_literals;
        switch (level) {
        case LogLevel::Disabled:
            return "Disabled"s;
        case LogLevel::Trace:
            return "Trace"s;
        case LogLevel::Debug:
            return "Debug"s;
        case LogLevel::Info:
            return "Info"s;
        case LogLevel::Warning:
            return "Warning"s;
        case LogLevel::Error:
            return "Error"s;
        }

        // Unreachable, but makes gcc happy
        return ""s;
    }

    inline std::ostream &operator<<(std::ostream &out, LogLevel level) {
        out << str(level);
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
} // namespace gg
} // namespace aws