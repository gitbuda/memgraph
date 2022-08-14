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

#include <iostream>
#include <thread>

#include <benchmark/benchmark.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;

static const std::size_t kThreadsNum = std::thread::hardware_concurrency();

class TestFixture : public benchmark::Fixture {
 protected:
  void SetUp(const benchmark::State &) { memgraph::utils::EnsureDir(test_folder_); }

  void TearDown(const benchmark::State &) { fs::remove_all(test_folder_); }

  fs::path test_folder_{fs::temp_directory_path() / ("manual_test_" + std::to_string(static_cast<int>(getpid())))};
  std::optional<memgraph::kvstore::KVStore> kvstore = std::nullopt;
};

BENCHMARK_DEFINE_F(TestFixture, TestRocks)
(benchmark::State &state) {
  if (state.thread_index() == 0) {
    if (!kvstore.has_value()) {
      boost::uuids::uuid unique_id;
      kvstore = memgraph::kvstore::KVStore(
          test_folder_ / ("TestRocks_" + std::to_string(state.threads()) + "_" + boost::uuids::to_string(unique_id)));
    }
  }
  uint64_t counter = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto key = boost::uuids::to_string(boost::uuids::random_generator()());
    auto value = boost::uuids::to_string(boost::uuids::random_generator()());
    state.ResumeTiming();
    kvstore->Put(key, value);
    counter += 1;
  }
  state.counters["rocks"] = counter;
  if (state.thread_index() == 0) {
    if (kvstore.has_value()) {
      std::cout << kvstore->Size() << std::endl;
      kvstore.reset();
    }
  }
}
BENCHMARK_REGISTER_F(TestFixture, TestRocks)->ThreadRange(1, kThreadsNum)->Unit(benchmark::kNanosecond)->UseRealTime();

BENCHMARK_MAIN();
