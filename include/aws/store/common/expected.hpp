// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#pragma once
#include <memory>

namespace aws {
namespace store {
namespace common {
template <class ExpectedT, class UnexpectedT> class Expected {
  private:
    bool _is_set{false};
    ExpectedT _val{};
    UnexpectedT _unexpected{};

  public:
    Expected(ExpectedT &&val) : _is_set(true), _val(std::move(val)) {
    }
    Expected(UnexpectedT &&unexpect) : _unexpected(std::move(unexpect)) {
    }
    // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
    Expected(const ExpectedT &val) : _is_set(true), _val(val) {
    }
    Expected(const UnexpectedT &unexpect) : _unexpected(unexpect) {
    }
    bool ok() const {
        return _is_set;
    }

    const ExpectedT &val() const & {
        return _val;
    }
    ExpectedT &val() & {
        return _val;
    }
    // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
    const ExpectedT &&val() const && {
        return std::move(_val);
    }
    // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
    ExpectedT &&val() && {
        return std::move(_val);
    }

    const UnexpectedT &err() const & {
        return _unexpected;
    }
    UnexpectedT &err() & {
        return _unexpected;
    }
    // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
    const UnexpectedT &&err() const && {
        return std::move(_unexpected);
    }
    // coverity[misra_cpp_2008_rule_14_7_1_violation] keep the more efficient method even if currently unused
    UnexpectedT &&err() && {
        return std::move(_unexpected);
    }
};
} // namespace common
} // namespace store
} // namespace aws
