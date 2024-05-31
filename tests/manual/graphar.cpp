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

#include <gar/api.h>
#include <gar/graph.h>
#include <gar/writer/edges_builder.h>
#include <gar/writer/vertices_builder.h>
#include <gflags/gflags.h>

#include "storage/custom_storage/gar_database.hpp"
#include "utils/logging.hpp"

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::trace);

  // "runtime" schema spec START
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

  memgraph::storage::custom_storage::GARDatabaseConfig::PerDatabase per_database = {
      .root = "/tmp/gar/",
      .version = std::make_shared<graphar::InfoVersion>(1),
  };
  const auto db_config = memgraph::storage::custom_storage::GARDatabaseConfig{
      .base = &per_database,
      .vertex_types = {{
          .base = &per_database,
          .label = "node",
          .properties = {group1, group2},
      }},
      .edge_types = {{
          .base = &per_database,
          .src_label = "node",
          .edge_type = "LINK",
          .dst_label = "node",
          .properties = {group3},
          .adjacent_lists = adjacent_lists,
      }},
  };
  // "runtime" schema spec START

  // init GAR
  auto &vertex_type = db_config.vertex_types[0];
  auto vertex_info = memgraph::storage::custom_storage::InitVertexTypes(db_config)[0];
  auto &edge_type = db_config.edge_types[0];
  auto edge_info = memgraph::storage::custom_storage::InitEdgeTypes(db_config)[0];

  // vertex data partition 1
  graphar::builder::VerticesBuilder builder(vertex_info, vertex_type.base->root, 0);
  builder.SetValidateLevel(graphar::ValidateLevel::strong_validate);
  int vertex_count = 2;
  std::vector<std::string> property_names = {"id", "domain"};
  std::vector<int64_t> id = {0, 1};
  std::vector<std::string> domain = {"google.com", "memgraph.com"};
  for (int i = 0; i < vertex_count; i++) {
    graphar::builder::Vertex v;
    v.AddProperty(property_names[0], id[i]);
    v.AddProperty(property_names[1], domain[i]);
    MG_ASSERT(builder.AddVertex(v).ok());
  }
  MG_ASSERT(builder.GetNum() == vertex_count);
  spdlog::info("vertex_count={}", builder.GetNum());
  MG_ASSERT(builder.Dump().ok());
  spdlog::info("dump vertices collection successfully!");
  builder.Clear();
  MG_ASSERT(builder.GetNum() == 0);
  // "runtime" schema spec START

  // vertex data partition 2 -> IMPORTANT: controlling start_vertex_index means partitioning & parallelization.
  graphar::builder::VerticesBuilder builder2(vertex_info, vertex_type.base->root,
                                             vertex_type.base->vertex_chunk_size * 1);
  builder.SetValidateLevel(graphar::ValidateLevel::strong_validate);
  vertex_count = 2;
  property_names = {"id", "domain", "extra"};
  id = {2, 3};
  domain = {"nvidia.com", "facebook.com"};
  std::vector<std::string> extra = {"{key:value}", "{}"};
  for (int i = 0; i < vertex_count; i++) {
    graphar::builder::Vertex v;
    v.AddProperty(property_names[0], id[i]);
    v.AddProperty(property_names[1], domain[i]);
    if (i == 0) {
      v.AddProperty(property_names[2], extra[0]);
    } else {
      v.AddProperty(property_names[2], extra[1]);
    }
    MG_ASSERT(builder2.AddVertex(v).ok());
  }
  MG_ASSERT(builder2.GetNum() == vertex_count);
  spdlog::info("vertex_count={}", builder2.GetNum());
  MG_ASSERT(builder2.Dump().ok());
  spdlog::info("dump vertices collection successfully!");
  builder2.Clear();
  MG_ASSERT(builder2.GetNum() == 0);

  // edge data
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

  // std::string path = "/tmp/todo/data.yml";
  // auto graph_info = graphar::GraphInfo::Load(path).value();
  // std::string label = "person";
  // auto maybe_vertices_collection =
  //     graphar::VerticesCollection::Make(graph_info, label);
  // MG_ASSERT(!maybe_vertices_collection.has_error());
  // auto vertices = maybe_vertices_collection.value();
  // auto tmp = vertices->find(0);

  return 0;
}
