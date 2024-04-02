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

#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>

#include <gar/api.h>
#include <gar/writer/edges_builder.h>
#include <gar/writer/vertices_builder.h>
#include <gflags/gflags.h>

#include "utils/logging.hpp"

struct GARDatabaseConfig {
  struct PerDatabase {
    std::filesystem::path root{std::filesystem::temp_directory_path()};  // single database root directory
    std::shared_ptr<graphar::InfoVersion> version;
    std::string vertex_metadata_suffix{".vertex.yaml"};
    std::string edge_metadata_suffix{".edge.yaml"};
    std::filesystem::path vertex_folder_prefix{"vertex"};
    std::filesystem::path edge_folder_prefix{"edge"};
    uint64_t vertex_chunk_size{1024};
    uint64_t edge_chunk_size{1024};
    uint64_t edge_src_chunk_size{1024};
    uint64_t edge_dst_chunk_size{1024};
    bool is_directed{false};
    graphar::AdjListType ordering;
  } * base;

  struct GARVertexType {
    PerDatabase *base{nullptr};
    void CheckBase() const { MG_ASSERT(base != nullptr); }
    std::string label;
    graphar::PropertyGroupVector properties;
    std::filesystem::path Prefix() const {
      CheckBase();
      return base->vertex_folder_prefix / std::filesystem::path(label);
    }
    std::string SavePath() const {
      CheckBase();
      return base->root / std::filesystem::path(label + base->vertex_metadata_suffix);
    }
  };
  std::vector<GARVertexType> vertex_types;

  struct GAREdgeType {
    PerDatabase *base{nullptr};
    void CheckBase() const { MG_ASSERT(base != nullptr); }
    std::string src_label;
    std::string edge_type;
    std::string dst_label;
    graphar::PropertyGroupVector properties;
    std::vector<std::shared_ptr<graphar::AdjacentList>> adjacent_lists;
    std::string src_type_dst{src_label + "__" + edge_type + "__" + dst_label};
    std::filesystem::path Prefix() const {
      CheckBase();
      return base->edge_folder_prefix / std::filesystem::path(src_type_dst);
    }
    std::string SavePath() const {
      CheckBase();
      return base->root / std::filesystem::path(src_type_dst + base->edge_metadata_suffix);
    }
  };
  std::vector<GAREdgeType> edge_types;
};

auto InitVertexType(const GARDatabaseConfig::GARVertexType &vertex_type) {
  auto vertex_info = graphar::CreateVertexInfo(vertex_type.label, vertex_type.base->vertex_chunk_size,
                                               vertex_type.properties, vertex_type.Prefix(), vertex_type.base->version);
  MG_ASSERT(!vertex_info->Dump().has_error());
  MG_ASSERT(vertex_info->Save(vertex_type.SavePath()).ok());
  return vertex_info;
}

auto InitVertexTypes(const GARDatabaseConfig &config) {
  std::vector<std::shared_ptr<graphar::VertexInfo>> vertex_infos;
  for (const auto &vertex_type : config.vertex_types) {
    auto vertex_info = InitVertexType(vertex_type);
    vertex_infos.push_back(vertex_info);
  }
  return vertex_infos;
}

auto InitEdgeType(const GARDatabaseConfig::GAREdgeType &edge_type) {
  auto edge_info = graphar::CreateEdgeInfo(
      edge_type.src_label, edge_type.edge_type, edge_type.dst_label, edge_type.base->edge_chunk_size,
      edge_type.base->edge_src_chunk_size, edge_type.base->edge_dst_chunk_size, edge_type.base->is_directed,
      edge_type.adjacent_lists, edge_type.properties, edge_type.Prefix(), edge_type.base->version);
  MG_ASSERT(!edge_info->Dump().has_error());
  MG_ASSERT(edge_info->Save(edge_type.SavePath()).ok());
  return edge_info;
}

auto InitEdgeTypes(const GARDatabaseConfig &config) {
  std::vector<std::shared_ptr<graphar::EdgeInfo>> edge_infos;
  for (const auto &edge_type : config.edge_types) {
    auto edge_info = InitEdgeType(edge_type);
    edge_infos.push_back(edge_info);
  }
  return edge_infos;
}

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

  GARDatabaseConfig::PerDatabase per_database = {
      .root = "/tmp/gar/",
      .version = std::make_shared<graphar::InfoVersion>(1),
  };
  const auto db_config = GARDatabaseConfig{
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
  auto vertex_info = InitVertexTypes(db_config)[0];
  auto &edge_type = db_config.edge_types[0];
  auto edge_info = InitEdgeTypes(db_config)[0];

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

  return 0;
}
