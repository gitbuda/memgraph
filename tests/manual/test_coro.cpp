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

#include <coroutine>  // clang #include <experimental/coroutine>
#include <exception>
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
  using handle_type = std::coroutine_handle<promise_type>;

  struct promise_type {  // required
    T value_;
    std::exception_ptr exception_;

    SyncGenerator get_return_object() { return SyncGenerator(handle_type::from_promise(*this)); }
    std::suspend_always initial_suspend() { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void unhandled_exception() {
      exception_ = std::current_exception();
    }  // saving
       // exception

    // template <std::convertible_to<T> From>  // C++20 concept
    template <typename From>
    std::suspend_always yield_value(From &&from) {
      value_ = std::forward<From>(from);  // caching the result in promise
      return {};
    }
    void return_void() {}
  };

  handle_type h_;

  SyncGenerator(handle_type h) : h_(h) {}
  ~SyncGenerator() { h_.destroy(); }
  explicit operator bool() {
    fill();  // The only way to reliably find out whether or not we finished coroutine,
             // whether or not there is going to be a next value generated (co_yield) in
             // coroutine via C++ getter (operator () below) is to execute/resume coroutine
             // until the next co_yield point (or let it fall off end).
             // Then we store/cache result in promise to allow getter (operator() below to
             // grab it without executing coroutine).
    return !h_.done();
  }
  T operator()() {
    fill();
    full_ = false;  // we are going to move out previously cached
                    // result to make promise empty again
    return std::move(h_.promise().value_);
  }

 private:
  bool full_ = false;
  void fill() {
    if (!full_) {
      h_();
      if (h_.promise().exception_) std::rethrow_exception(h_.promise().exception_);
      // propagate coroutine exception in called context
      full_ = true;
    }
  }
};

SyncGenerator<uint64_t> fibonacci_sequence(unsigned n) {
  if (n == 0) co_return;
  if (n > 94) throw std::runtime_error("Too big Fibonacci sequence. Elements would overflow.");

  co_yield 0;
  if (n == 1) co_return;
  co_yield 1;
  if (n == 2) co_return;

  uint64_t a = 0;
  uint64_t b = 1;
  for (unsigned i = 2; i < n; i++) {
    uint64_t s = a + b;
    co_yield s;
    a = b;
    b = s;
  }
}

int main() {
  try {
    auto gen = fibonacci_sequence(10);  // max 94 before uint64_t overflows

    for (int j = 0; gen; j++) std::cout << "fib(" << j << ")=" << gen() << '\n';

  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << '\n';
  } catch (...) {
    std::cerr << "Unknown exception.\n";
  }
}
