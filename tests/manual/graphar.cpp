// Copyright 2024 Memgraph Ltd.
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
#include <memory>

#include <gar/api.h>
#include <gflags/gflags.h>

#define GRAPH_NAME "manual_graph"
#define IS_DIRECTED false
#define SAVE_PATH "/tmp/" + graph_name + "/"
#define ADJLIST_TYPE GAR_NAMESPACE_INTERNAL::AdjListType::ordered_by_source
#define PAYLOAD_TYPE GAR_NAMESPACE_INTERNAL::FileType::CSV
#define VERTEX_CHUNK_SIZE 1024
#define EDGE_CHUNK_SIZE 1024 * 1024

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // TODO(gitbuda): Add logging.

  std::string graph_name = GRAPH_NAME;
  std::string save_path = SAVE_PATH;
  // TODO(gitbuda): Parsing of string version doesn't work for some reason, try with toolchain-v5.
  auto version = std::make_shared<GAR_NAMESPACE_INTERNAL::InfoVersion>(1);
  std::string vertex_label = "node";
  std::string vertex_prefix = "vertex/node/";
  auto vertex_info =
      GAR_NAMESPACE_INTERNAL::CreateVertexInfo(vertex_label, VERTEX_CHUNK_SIZE, {}, vertex_prefix, version);
  vertex_info->Dump().error();
  vertex_info->Save(save_path + "node.vertex.yaml").ok();

  return 0;
}
