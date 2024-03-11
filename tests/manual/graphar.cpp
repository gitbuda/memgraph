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

#include <gar/api.h>
#include <gar/writer/edges_builder.h>
#include <gar/writer/vertices_builder.h>
#include <gflags/gflags.h>

#include "utils/logging.hpp"

struct GARDatabaseConfig {
  std::filesystem::path root{std::filesystem::temp_directory_path()};  // single database root directory
  std::string vertex_metadata_suffix{".vertex.yaml"};
  std::filesystem::path vertex_folder_prefix{"vertex"};
  uint64_t vertex_chunk_size{1024};
  uint64_t edge_chunk_size{1024 * 1024};
  bool is_directed{true};
  GAR_NAMESPACE_INTERNAL::AdjListType ordering;
};

struct GARVertexType {
  GARDatabaseConfig base;
  std::string label;
  std::shared_ptr<GAR_NAMESPACE_INTERNAL::InfoVersion> version;
  GAR_NAMESPACE_INTERNAL::PropertyGroupVector properties;
  std::string SavePath() const { return base.root / std::filesystem::path(label + base.vertex_metadata_suffix); }
  std::filesystem::path Prefix() const { return base.vertex_folder_prefix / std::filesystem::path(label); }
};

auto InitVertexType(const GARVertexType &type) {
  auto vertex_info = GAR_NAMESPACE_INTERNAL::CreateVertexInfo(type.label, type.base.vertex_chunk_size, type.properties,
                                                              type.Prefix(), type.version);
  MG_ASSERT(!vertex_info->Dump().has_error());
  MG_ASSERT(vertex_info->Save(type.SavePath()).ok());
  return vertex_info;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::trace);

  // schema
  const auto database = GARDatabaseConfig{.root = "/tmp/gar/"};
  auto property_vector_1 = {GAR_NAMESPACE_INTERNAL::Property("id", GAR_NAMESPACE_INTERNAL::int64(), true)};
  auto property_vector_2 = {GAR_NAMESPACE_INTERNAL::Property("domain", GAR_NAMESPACE_INTERNAL::string(), false),
                            GAR_NAMESPACE_INTERNAL::Property("extra", GAR_NAMESPACE_INTERNAL::string(), false)};
  auto group1 = GAR_NAMESPACE_INTERNAL::CreatePropertyGroup(property_vector_1, GAR_NAMESPACE_INTERNAL::FileType::CSV);
  auto group2 = GAR_NAMESPACE_INTERNAL::CreatePropertyGroup(property_vector_2, GAR_NAMESPACE_INTERNAL::FileType::CSV);
  const auto vertex_type = GARVertexType{.base = database,
                                         .label = "node",
                                         .version = std::make_shared<GAR_NAMESPACE_INTERNAL::InfoVersion>(1),
                                         .properties = {group1, group2}};

  // data partition 1
  auto vertex_info = InitVertexType(vertex_type);
  GAR_NAMESPACE_INTERNAL::IdType start_index = 0;
  GAR_NAMESPACE_INTERNAL::builder::VerticesBuilder builder(vertex_info, vertex_type.base.root, 0);
  builder.SetValidateLevel(GAR_NAMESPACE_INTERNAL::ValidateLevel::strong_validate);
  int vertex_count = 2;
  std::vector<std::string> property_names = {"id", "domain"};
  std::vector<int64_t> id = {0, 1};
  std::vector<std::string> domain = {"google.com", "memgraph.com"};
  for (int i = 0; i < vertex_count; i++) {
    GAR_NAMESPACE_INTERNAL::builder::Vertex v;
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
  // data partition 2 -> IMPORTANT: controlling start_vertex_index means partitioning & parallelization.
  GAR_NAMESPACE_INTERNAL::builder::VerticesBuilder builder2(vertex_info, vertex_type.base.root,
                                                            vertex_type.base.vertex_chunk_size * 1);
  builder.SetValidateLevel(GAR_NAMESPACE_INTERNAL::ValidateLevel::strong_validate);
  vertex_count = 2;
  property_names = {"id", "domain", "extra"};
  id = {2, 3};
  domain = {"nvidia.com", "facebook.com"};
  std::vector<std::string> extra = {"{key:value}", "{}"};
  for (int i = 0; i < vertex_count; i++) {
    GAR_NAMESPACE_INTERNAL::builder::Vertex v;
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

  return 0;
}
