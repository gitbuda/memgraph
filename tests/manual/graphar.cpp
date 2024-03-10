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

#include <gar/api.h>
#include <gflags/gflags.h>

#define GRAPH_NAME "manual_graph"
#define IS_DIRECTED false
#define SAVE_PATH "/tmp/" + graph_name + "/"
#define ADJLIST_TYPE GAR_NAMESPACE::AdjListType::ordered_by_source
#define PAYLOAD_TYPE GAR_NAMESPACE::FileType::CSV
#define VERTEX_CHUNK_SIZE 1024
#define EDGE_CHUNK_SIZE 1024 * 1024

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string graph_name = GRAPH_NAME;
  std::string save_path = SAVE_PATH;
  auto version = GAR_NAMESPACE::InfoVersion::Parse("gar/v1").value();
  std::string vertex_label = "node", vertex_prefix = "vertex/node/";
  auto vertex_info = GAR_NAMESPACE::CreateVertexInfo(vertex_label, VERTEX_CHUNK_SIZE, {}, vertex_prefix, version);
  ASSERT(!vertex_info->Dump().has_error());
  ASSERT(vertex_info->Save(save_path + "node.vertex.yaml").ok());

  std::cout << "GraphAr test" << std::endl;

  return 0;
}
