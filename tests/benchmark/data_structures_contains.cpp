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

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <set>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include <benchmark/benchmark.h>
#include <gflags/gflags.h>

#include "data_structures_common.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/lexicographically_ordered_vertex.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::benchmark {

///////////////////////////////////////////////////////////////////////////////
// Testing Contains Operation
///////////////////////////////////////////////////////////////////////////////
static void BM_BenchmarkContainsSkipList(::benchmark::State &state) {
  utils::SkipList<storage::v3::PrimaryKey> skip_list;
  PrepareData(skip_list, state.range(0));
  // So we can also have elements that does don't exist
  std::mt19937 i_generator(std::random_device{}());
  std::uniform_int_distribution<int64_t> i_distribution(0, state.range(0) * 2);
  int64_t found_elems{0};
  for (auto _ : state) {
    for (auto i{0}; i < state.range(0); ++i) {
      int64_t value = i_distribution(i_generator);
      auto acc = skip_list.access();
      if (acc.contains(storage::v3::PrimaryKey{{storage::v3::PropertyValue(value)}})) {
        found_elems++;
      }
    }
  }
  state.SetItemsProcessed(found_elems);
}

static void BM_BenchmarkContainsStdMap(::benchmark::State &state) {
  std::map<storage::v3::PrimaryKey, storage::v3::LexicographicallyOrderedVertex> std_map;
  PrepareData(std_map, state.range(0));

  // So we can also have elements that does don't exist
  std::mt19937 i_generator(std::random_device{}());
  std::uniform_int_distribution<int64_t> i_distribution(0, state.range(0) * 2);
  int64_t found_elems{0};
  for (auto _ : state) {
    for (auto i{0}; i < state.range(0); ++i) {
      int64_t value = i_distribution(i_generator);
      if (std_map.contains(storage::v3::PrimaryKey{{storage::v3::PropertyValue(value)}})) {
        found_elems++;
      }
    }
  }
  state.SetItemsProcessed(found_elems);
}

static void BM_BenchmarkContainsStdSet(::benchmark::State &state) {
  std::set<storage::v3::PrimaryKey> std_set;
  PrepareData(std_set, state.range(0));

  // So we can also have elements that does don't exist
  std::mt19937 i_generator(std::random_device{}());
  std::uniform_int_distribution<int64_t> i_distribution(0, state.range(0) * 2);
  int64_t found_elems{0};
  for (auto _ : state) {
    for (auto i{0}; i < state.range(0); ++i) {
      int64_t value = i_distribution(i_generator);
      if (std_set.contains(storage::v3::PrimaryKey{storage::v3::PropertyValue{value}})) {
        found_elems++;
      }
    }
  }
  state.SetItemsProcessed(found_elems);
}

BENCHMARK(BM_BenchmarkContainsSkipList)->RangeMultiplier(10)->Range(1000, 10000000)->Unit(::benchmark::kMillisecond);

BENCHMARK(BM_BenchmarkContainsStdMap)->RangeMultiplier(10)->Range(1000, 10000000)->Unit(::benchmark::kMillisecond);

BENCHMARK(BM_BenchmarkContainsStdSet)->RangeMultiplier(10)->Range(1000, 10000000)->Unit(::benchmark::kMillisecond);

}  // namespace memgraph::benchmark

BENCHMARK_MAIN();
