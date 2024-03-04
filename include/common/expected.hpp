#pragma once
#include <memory>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    template <class ExpectedT, class UnexpectedT> class expected {
      private:
        bool _is_set{false};
        ExpectedT _val;
        UnexpectedT _unexpected;

      public:
        expected(ExpectedT &&val) : _is_set(true), _val(std::move(val)) {}
        expected(UnexpectedT &&unexpect) : _unexpected(std::move(unexpect)) {}
        expected(const ExpectedT &val) : _is_set(true), _val(val) {}
        expected(const UnexpectedT &unexpect) : _unexpected(unexpect) {}
        bool ok() const { return _is_set; }

        const ExpectedT &val() const & { return _val; }
        ExpectedT &val() & { return _val; }
        const ExpectedT &&val() const && { return std::move(_val); }
        ExpectedT &&val() && { return std::move(_val); }

        const UnexpectedT &err() const & { return _unexpected; }
        UnexpectedT &err() & { return _unexpected; }
        const UnexpectedT &&err() const && { return std::move(_unexpected); }
        UnexpectedT &&err() && { return std::move(_unexpected); }
    };
} // namespace gg
} // namespace aws