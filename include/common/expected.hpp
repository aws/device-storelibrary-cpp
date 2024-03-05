#pragma once
#include <memory>

namespace aws {
namespace gg __attribute__((visibility("default"))) {
    template <class ExpectedT, class UnexpectedT> class expected {
      private:
        bool _is_set{false};
        ExpectedT _val{};
        UnexpectedT _unexpected{};

      public:
        expected(ExpectedT &&val) : _is_set(true), _val(std::move(val)) {}
        expected(UnexpectedT &&unexpect) : _unexpected(std::move(unexpect)) {}
        // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
        expected(const ExpectedT &val) : _is_set(true), _val(val) {}
        expected(const UnexpectedT &unexpect) : _unexpected(unexpect) {}
        bool ok() const { return _is_set; }

        const ExpectedT &val() const & { return _val; }
        ExpectedT &val() & { return _val; }
        // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
        const ExpectedT &&val() const && { return std::move(_val); }
        // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
        ExpectedT &&val() && { return std::move(_val); }

        const UnexpectedT &err() const & { return _unexpected; }
        UnexpectedT &err() & { return _unexpected; }
        // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
        const UnexpectedT &&err() const && { return std::move(_unexpected); }
        // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
        UnexpectedT &&err() && { return std::move(_unexpected); }
    };
} // namespace gg
} // namespace aws