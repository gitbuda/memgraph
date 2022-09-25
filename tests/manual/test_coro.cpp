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
#include <experimental/coroutine>  // clang #include <experimental/coroutine>
#include <iostream>
// #include <concepts> // because of std::convertible_to
// apt install libc++-10-dev libc++abi-10-dev
// CLANG 10: std::experimental
// clang++ -std=c++20 -stdlib=libc++ -fcoroutines-ts test_coro.cpp
//                         |------------|
//                               |
//                               --> big deal because Memgraph toolchain is missing libc++
// TODO(gitbuda): Add libc++ to the toolchain -> https://libcxx.llvm.org/BuildingLibcxx.html
//
// GCC 11.2: just std
// g++ -std=c++2a -fcoroutines test_coro.cpp
// TODO(gitbuda): Test compile Memgraph with GCC again
//
// NOTE: Replacement for the code here -> https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p2168r3.pdf

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

SyncGenerator<int> CreateDummyGenerator(int *ptr, int max) {
  for (int i = 0; i < max; ++i) {
    std::cout << "Call i: " << i << std::endl;
    co_yield i;
  }
  co_return max;
}

int main() {
  try {
    auto ptr = std::make_unique<int>(10);
    auto gen1 = CreateDummyGenerator(ptr.get(), 10);
    for (; gen1;) {
      auto gen2 = CreateDummyGenerator(ptr.get(), 5);
      for (; gen2;) {
        std::cout << gen1() * gen2() << std::endl;
      }
    }
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << '\n';
  } catch (...) {
    std::cerr << "Unknown exception.\n";
  }
}
