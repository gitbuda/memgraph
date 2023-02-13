// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <istream>
#include <thread>

#include <gflags/gflags.h>
#include <spdlog/cfg/env.h>
#include <spdlog/spdlog.h>

// v3 includes
#include "io/address.hpp"
#include "io/local_transport/local_system.hpp"
#include "io/message_histogram_collector.hpp"
#include "machine_manager/machine_manager.hpp"
#include "query/discard_value_stream.hpp"
#include "query/v2/discard_value_stream.hpp"
#include "query/v2/interpreter.hpp"
#include "query/v2/request_router.hpp"

// v2 includes
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(index_queries_file, "",
              "Path to the file which contains the queries to create indices. Used only for v2.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(split_file, "",
              "Path to the split file which contains the predefined labels, properties, edge types and shard-ranges. "
              "Used only for v3.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(init_queries_file, "", "Path to the file that is used to insert the initial dataset.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(benchmark_queries_file, "", "Path to the file that contains the queries that we want to measure.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(use_v3, true, "If set to true, then Memgraph v3 will be used, otherwise Memgraph v2 will be used.");

namespace memgraph::tests::manual {

template <typename TInterpreterContext>
struct DependantTypes {};

template <>
struct DependantTypes<query::InterpreterContext> {
  using Interpreter = query::Interpreter;
  using DiscardValueResultStream = query::DiscardValueResultStream;
};

template <>
struct DependantTypes<query::v2::InterpreterContext> {
  using Interpreter = query::v2::Interpreter;
  using DiscardValueResultStream = query::v2::DiscardValueResultStream;
};

template <typename TInterpreterContext>
using Interpreter = typename DependantTypes<TInterpreterContext>::Interpreter;

template <typename TInterpreterContext>
using DiscardValueResultStream = typename DependantTypes<TInterpreterContext>::DiscardValueResultStream;

template <typename TInterpreterContext>
void RunQueries(TInterpreterContext &interpreter_context, const std::vector<std::string> &queries) {
  Interpreter<TInterpreterContext> interpreter{&interpreter_context};
  DiscardValueResultStream<TInterpreterContext> stream;

  for (const auto &query : queries) {
    auto result = interpreter.Prepare(query, {}, nullptr);
    interpreter.Pull(&stream, std::nullopt, result.qid);
  }
}

template <typename TInterpreterContext>
void RunInitQueries(TInterpreterContext &interpreter_context, const std::vector<std::string> &init_queries) {
  RunQueries(interpreter_context, init_queries);
}

template <typename TInterpreterContext>
void RunBenchmarkQueries(TInterpreterContext &interpreter_context, const std::vector<std::string> &benchmark_queries) {
  RunQueries(interpreter_context, benchmark_queries);
}

void RunV2() {
  const auto run_start = std::chrono::high_resolution_clock::now();

  std::vector<std::string> init_queries{};
  std::string buffer;

  std::ifstream indices_file{FLAGS_index_queries_file, std::ios::in};
  MG_ASSERT(indices_file.good(), "Cannot open index queries file to read: {}", FLAGS_index_queries_file);
  while (indices_file.good()) {
    std::getline(indices_file, buffer);
    if (buffer.empty()) {
      continue;
    }
    // Trim the trailing `;`
    init_queries.push_back(buffer.substr(0, buffer.size() - 1));
  }

  std::ifstream init_file{FLAGS_init_queries_file, std::ios::in};
  MG_ASSERT(init_file.good(), "Cannot open init queries file to read: {}", FLAGS_init_queries_file);
  while (init_file.good()) {
    std::getline(init_file, buffer);
    if (buffer.empty()) {
      continue;
    }
    // Trim the trailing `;`
    init_queries.push_back(buffer.substr(0, buffer.size() - 1));
  }

  std::ifstream benchmark_file{FLAGS_benchmark_queries_file, std::ios::in};
  MG_ASSERT(benchmark_file.good(), "Cannot open benchmark queries file to read: {}", FLAGS_benchmark_queries_file);
  std::vector<std::string> benchmark_queries{};

  while (benchmark_file.good()) {
    std::getline(benchmark_file, buffer);
    if (buffer.empty()) {
      continue;
    }
    // Trim the trailing `;`
    benchmark_queries.push_back(buffer.substr(0, buffer.size() - 1));
  }
  storage::Storage storage{
      storage::Config{.durability{.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED}}};

  memgraph::query::InterpreterContext interpreter_context{
      &storage,
      {.query = {.allow_load_csv = false},
       .execution_timeout_sec = 0,
       .replication_replica_check_frequency = std::chrono::seconds(0),
       .default_kafka_bootstrap_servers = "",
       .default_pulsar_service_url = "",
       .stream_transaction_conflict_retries = 0,
       .stream_transaction_retry_interval = std::chrono::milliseconds(0)},
      "query_performance_data"};

  const auto init_start = std::chrono::high_resolution_clock::now();
  RunInitQueries(interpreter_context, init_queries);
  const auto benchmark_start = std::chrono::high_resolution_clock::now();
  spdlog::critical("Read: {}ms", std::chrono::duration_cast<std::chrono::milliseconds>(init_start - run_start).count());
  RunBenchmarkQueries(interpreter_context, benchmark_queries);
  const auto benchmark_end = std::chrono::high_resolution_clock::now();

  spdlog::critical("Init: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_start - init_start).count());
  spdlog::critical("Benchmark: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start).count());
}

io::LatencyHistogramSummaries RunV3() {
  const auto run_start = std::chrono::high_resolution_clock::now();
  std::ifstream sm_file{FLAGS_split_file, std::ios::in};
  MG_ASSERT(sm_file.good(), "Cannot open split file to read: {}", FLAGS_split_file);
  auto sm = memgraph::coordinator::ShardMap::Parse(sm_file);

  std::ifstream init_file{FLAGS_init_queries_file, std::ios::in};
  MG_ASSERT(init_file.good(), "Cannot open init queries file to read: {}", FLAGS_init_queries_file);
  std::vector<std::string> init_queries{};
  std::string buffer;
  while (init_file.good()) {
    std::getline(init_file, buffer);
    if (buffer.empty()) {
      continue;
    }
    // Trim the trailing `;`
    init_queries.push_back(buffer.substr(0, buffer.size() - 1));
  }

  std::ifstream benchmark_file{FLAGS_benchmark_queries_file, std::ios::in};
  MG_ASSERT(benchmark_file.good(), "Cannot open benchmark queries file to read: {}", FLAGS_benchmark_queries_file);
  std::vector<std::string> benchmark_queries{};

  while (benchmark_file.good()) {
    std::getline(benchmark_file, buffer);
    if (buffer.empty()) {
      continue;
    }
    // Trim the trailing `;`
    benchmark_queries.push_back(buffer.substr(0, buffer.size() - 1));
  }

  io::local_transport::LocalSystem ls;

  auto unique_local_addr_query = io::Address::UniqueLocalAddress();
  auto io = ls.Register(unique_local_addr_query);

  memgraph::machine_manager::MachineConfig config{
      .coordinator_addresses = std::vector<memgraph::io::Address>{unique_local_addr_query},
      .is_storage = true,
      .is_coordinator = true,
      .listen_ip = unique_local_addr_query.last_known_ip,
      .listen_port = unique_local_addr_query.last_known_port,
      .shard_worker_threads = 2,
  };

  memgraph::coordinator::Coordinator coordinator{sm};

  memgraph::machine_manager::MachineManager<memgraph::io::local_transport::LocalTransport> mm{io, config, coordinator};
  std::jthread mm_thread([&mm] { mm.Run(); });

  auto rr_factory = std::make_unique<memgraph::query::v2::LocalRequestRouterFactory>(io);

  query::v2::InterpreterContext interpreter_context{(memgraph::storage::v3::Shard *)(nullptr),
                                                    {.execution_timeout_sec = 0},
                                                    "data",
                                                    std::move(rr_factory),
                                                    mm.CoordinatorAddress()};

  // without this it fails sometimes because the CreateVertices request might reach the shard worker faster than the
  // ShardToInitialize
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  const auto init_start = std::chrono::high_resolution_clock::now();
  RunInitQueries(interpreter_context, init_queries);
  const auto benchmark_start = std::chrono::high_resolution_clock::now();
  RunBenchmarkQueries(interpreter_context, benchmark_queries);
  const auto benchmark_end = std::chrono::high_resolution_clock::now();

  spdlog::critical("Read: {}ms", std::chrono::duration_cast<std::chrono::milliseconds>(init_start - run_start).count());
  spdlog::critical("Init: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_start - init_start).count());
  spdlog::critical("Benchmark: {}ms",
                   std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start).count());
  ls.ShutDown();
  return io.ResponseLatencies();
}
}  // namespace memgraph::tests::manual

int main(int argc, char **argv) {
  spdlog::cfg::load_env_levels();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_use_v3) {
    memgraph::tests::manual::RunV3();
  } else {
    memgraph::tests::manual::RunV2();
  }
  return 0;
}
