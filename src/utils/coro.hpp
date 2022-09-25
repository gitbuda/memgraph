// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <exception>
#include <experimental/coroutine>

template <typename T>
struct SyncGenerator {
  struct promise_type;
  using handle_type = std::experimental::coroutine_handle<promise_type>;
  handle_type h_;

  struct promise_type {
    T value_;
    std::exception_ptr exception_;
    SyncGenerator get_return_object() { return SyncGenerator(handle_type::from_promise(*this)); }
    std::experimental::suspend_always initial_suspend() { return {}; }
    std::experimental::suspend_always final_suspend() noexcept { return {}; }
    void unhandled_exception() { exception_ = std::current_exception(); }
    template <typename From>
    std::experimental::suspend_always yield_value(From &&from) {
      std::cout << "yield value RVALUE" << std::endl;
      value_ = std::forward<From>(from);  // caching the result in promise
      return {};
    }
    void return_value(T &&value) {
      value_ = std::forward<T>(value);
      return;
    }
    void return_value(const T &value) {
      value_ = value;
      return;
    }
  };

  SyncGenerator(handle_type h) : h_(h) {}
  ~SyncGenerator() { h_.destroy(); }
  explicit operator bool() {
    fill();
    return !h_.done();
  }
  T operator()() {
    fill();
    full_ = false;
    return std::move(h_.promise().value_);
  }

 private:
  bool full_ = false;
  void fill() {
    if (!full_) {
      h_();
      if (h_.promise().exception_) std::rethrow_exception(h_.promise().exception_);
      full_ = true;
    }
  }
};
