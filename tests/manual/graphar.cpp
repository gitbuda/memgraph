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

#include <memory>
#include <vector>

#include "gflags/gflags.h"
#include "graphar/graph.h"
#include "graphar/writer/edges_builder.h"
#include "graphar/writer/vertices_builder.h"

#include "storage/custom_storage/gar_database.hpp"
#include "utils/logging.hpp"

// https://github.com/apache/incubator-graphar
// https://graphar.apache.org/docs/specification/implementation-status

auto GraphSchema() {
  // node metadata
  auto property_vector_1 = {graphar::Property("id", graphar::int64(), true)};
  auto property_vector_2 = {graphar::Property("domain", graphar::string(), false),
                            graphar::Property("extra", graphar::string(), false)};
  auto group1 = graphar::CreatePropertyGroup(property_vector_1, graphar::FileType::CSV);
  auto group2 = graphar::CreatePropertyGroup(property_vector_2, graphar::FileType::CSV);
  // edge metadata
  auto adjacent_lists = {graphar::CreateAdjacentList(graphar::AdjListType::ordered_by_source, graphar::FileType::CSV),
                         graphar::CreateAdjacentList(graphar::AdjListType::ordered_by_dest, graphar::FileType::CSV)};
  auto property_vector_3 = {graphar::Property("created", graphar::string(), false)};
  auto group3 = graphar::CreatePropertyGroup(property_vector_3, graphar::FileType::CSV);

  using PerDatabase = memgraph::storage::custom_storage::GARDatabaseConfig::PerDatabase;
  auto per_database = std::make_shared<PerDatabase>(PerDatabase{
      .root = "/tmp/gar/",
      .version = std::make_shared<graphar::InfoVersion>(1),
      .graph_name = "test",
      .vertex_chunk_size = 4,
  });
  return memgraph::storage::custom_storage::GARDatabaseConfig{
      .base = per_database,
      .vertex_types = {{
          .base = per_database,
          .label = "node",
          .properties = {group1, group2},
      }},
      .edge_types = {{
          .base = per_database,
          .src_label = "node",
          .edge_type = "LINK",
          .dst_label = "node",
          .properties = {group3},
          .adjacent_lists = adjacent_lists,
      }},
  };
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::trace);

  auto db_config = GraphSchema();
  auto &vertex_type = db_config.vertex_types[0];
  auto vertex_infos = memgraph::storage::custom_storage::InitVertexTypes(db_config);
  auto vertex_info = vertex_infos[0];
  auto &edge_type = db_config.edge_types[0];
  auto edge_infos = memgraph::storage::custom_storage::InitEdgeTypes(db_config);
  auto edge_info = edge_infos[0];
  auto graph_info = memgraph::storage::custom_storage::InitGraph(db_config, vertex_infos, edge_infos);
  spdlog::info("GAR initialized");

  spdlog::info("== CREATE ==");
  // CREATE vertex data partition 1
  graphar::builder::VerticesBuilder builder(vertex_info, vertex_type.base->root, 0);
  builder.SetValidateLevel(graphar::ValidateLevel::strong_validate);
  int vertex_count = 4;
  std::vector<std::string> property_names = {"id", "domain", "extra"};
  std::vector<int64_t> id = {0, 1, 2, 3};
  std::vector<std::string> domain = {"google.com", "memgraph.com", "nvidia.com", "facebook.com"};
  std::vector<std::string> extra = {"{key:value}", "{}", "", ""};
  for (int i = 0; i < vertex_count; i++) {
    graphar::builder::Vertex v;
    v.AddProperty(property_names[0], id[i % 4]);
    v.AddProperty(property_names[1], domain[i % 4]);
    v.AddProperty(property_names[2], extra[i % 4]);
    MG_ASSERT(builder.AddVertex(v).ok());
  }
  MG_ASSERT(builder.GetNum() == vertex_count);
  spdlog::info("vertex_count={}", builder.GetNum());
  MG_ASSERT(builder.Dump().ok());
  spdlog::info("dump vertices collection successfully!");
  builder.Clear();
  MG_ASSERT(builder.GetNum() == 0);

  // TODO: CREATE vertex data partition 2
  // IMPORTANT: controlling start_vertex_index means partitioning & parallelization,
  // BUT it only works if all chunks are monotonically populated
  // (from 0 to total vertex_count, no missing vertices).
  // The way how how VerticesBuilder is storing size is limiting
  // because it just overrides the number of nodes during the Dump call.
  // NOTE: It's possible to use low-level primitives to achieve parallelization.
  graphar::builder::VerticesBuilder builder2(vertex_info, vertex_type.base->root,
                                             vertex_type.base->vertex_chunk_size * 1);
  builder.SetValidateLevel(graphar::ValidateLevel::strong_validate);
  vertex_count = 2;
  property_names = {"id", "domain", "extra"};
  id = {2, 3};
  domain = {"nvidia.com", "facebook.com"};
  std::vector<std::string> extra2 = {"{key:value}", "{}"};
  for (int i = 0; i < vertex_count; i++) {
    graphar::builder::Vertex v;
    v.AddProperty(property_names[0], id[i]);
    v.AddProperty(property_names[1], domain[i]);
    if (i == 0) {
      v.AddProperty(property_names[2], extra2[0]);
    } else {
      v.AddProperty(property_names[2], extra2[1]);
    }
    MG_ASSERT(builder2.AddVertex(v).ok());
  }
  MG_ASSERT(builder2.GetNum() == vertex_count);
  spdlog::info("vertex_count={}", builder2.GetNum());
  MG_ASSERT(builder2.Dump().ok());
  spdlog::info("dump vertices collection successfully!");
  builder2.Clear();
  MG_ASSERT(builder2.GetNum() == 0);

  // CREATE edge data
  graphar::builder::EdgesBuilder builder3(edge_info, edge_type.base->root, graphar::AdjListType::ordered_by_dest, 1025);
  builder.SetValidateLevel(graphar::ValidateLevel::strong_validate);
  int edge_count = 4;
  property_names = {"created"};
  std::vector<int64_t> src = {1, 0, 0, 2};
  std::vector<int64_t> dst = {0, 1, 2, 1};
  std::vector<std::string> creationDate = {"2010-01-01", "2011-01-01", "2012-01-01", "2013-01-01"};
  for (int i = 0; i < edge_count; i++) {
    graphar::builder::Edge e(src[i], dst[i]);
    e.AddProperty("created", creationDate[i]);
    MG_ASSERT(builder3.AddEdge(e).ok());
  }
  MG_ASSERT(builder3.GetNum() == edge_count);
  spdlog::info("edge_count={}", builder3.GetNum());
  MG_ASSERT(builder3.Dump().ok());
  spdlog::info("dump edges collection successfully!");
  builder3.Clear();
  MG_ASSERT(builder3.GetNum() == 0);

  spdlog::info("== READ ==");
  // MATCH vertex data
  std::string graph_metadata_path = vertex_type.SavePath();
  auto maybe_vertices = graphar::VerticesCollection::Make(graph_info, vertex_type.label);
  auto vertices = maybe_vertices.value();
  spdlog::info(vertices->size());
  auto v_it_begin = vertices->begin(), v_it_end = vertices->end();
  for (auto it = v_it_begin; it != v_it_end; ++it) {
    auto vertex = *it;
    spdlog::info(std::to_string(vertex.property<int64_t>("id").value()) + " " +
                 vertex.property<std::string>("domain").value() + " " + vertex.property<std::string>("extra").value());
  }

  return 0;
}
