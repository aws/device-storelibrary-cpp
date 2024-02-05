#pragma once
#include <memory>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    template <class ExpectedT, class UnexpectedT> class expected {
      private:
        ExpectedT _val;
        UnexpectedT _unexpected;
        bool _is_set{false};

      public:
        expected(ExpectedT &&val) : _is_set(true), _val(std::move(val)) {}
        expected(UnexpectedT &&unexpect) : _is_set(false), _unexpected(std::move(unexpect)) {}
        explicit operator bool() const { return _is_set; }

        [[nodiscard]] const ExpectedT &val() const & { return _val; }
        [[nodiscard]] ExpectedT &val() & { return _val; }
        [[nodiscard]] const ExpectedT &&val() const && { return std::move(_val); }
        [[nodiscard]] ExpectedT &&val() && { return std::move(_val); }

        [[nodiscard]] const UnexpectedT &err() const & { return _unexpected; }
        [[nodiscard]] UnexpectedT &err() & { return _unexpected; }
        [[nodiscard]] const UnexpectedT &&err() const && { return std::move(_unexpected); }
        [[nodiscard]] UnexpectedT &&err() && { return std::move(_unexpected); }
    };
} // namespace gg
} // namespace aws