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

// NOTE: https://github.com/google/benchmark/blob/main/docs/user_guide.md#a-faster-keeprunning-loop

#include <iostream>
#include <string>
#include <thread>

#include <benchmark/benchmark.h>

static const std::size_t kThreadsNum = std::thread::hardware_concurrency();

class HashingResearchFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &) override {
    // pass
  }
};

#include "xxHash/xxhash.h"
BENCHMARK_DEFINE_F(HashingResearchFixture, YC)
(benchmark::State &state) {
  uint64_t counter = 0;
  std::string input = "short property name";
  for (auto _ : state) {
    XXH64(input.c_str(), input.size(), 0);
    counter += 1;
  }
  state.counters["hashes"] = counter;
}
BENCHMARK_REGISTER_F(HashingResearchFixture, YC)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

#include "xxhashct/xxh64.hpp"
BENCHMARK_DEFINE_F(HashingResearchFixture, DK)
(benchmark::State &state) {
  uint64_t counter = 0;
  std::string input = "short property name";
  for (auto _ : state) {
    xxh64::hash(input.c_str(), input.size(), 0);
    counter += 1;
  }
  state.counters["hashes"] = counter;
}
BENCHMARK_REGISTER_F(HashingResearchFixture, DK)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

#include "xxhash/xxhash64.h"
BENCHMARK_DEFINE_F(HashingResearchFixture, SB)
(benchmark::State &state) {
  uint64_t counter = 0;
  std::string input = "short property name";
  for (auto _ : state) {
    XXHash64::hash(input.c_str(), input.size(), 0);
    counter += 1;
  }
  state.counters["hashes"] = counter;
}
BENCHMARK_REGISTER_F(HashingResearchFixture, SB)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

BENCHMARK_MAIN();
