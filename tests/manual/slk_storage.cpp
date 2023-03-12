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

#include <gflags/gflags.h>

#include "slk/loopback.hpp"
#include "storage/rocks/serialization.hpp"
#include "utils/logging.hpp"

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::info);

  memgraph::slk::Loopback loopback;
  memgraph::storage::rocks::Encoder encoder(loopback.GetBuilder());
  memgraph::storage::rocks::Vertex vertex{.id = 1234};
  encoder.WriteVertex(vertex);
  // HERE -> we have writeable data for RocksDB

  // HERE -> once we read data from RocksDB we can decode
  memgraph::storage::rocks::Decoder decoder(loopback.GetReader());
  auto maybe_vertex = decoder.ReadVertex();
  MG_ASSERT(maybe_vertex->id == 1234);
  return 0;
}
