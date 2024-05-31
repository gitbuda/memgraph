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

#pragma once

#include <filesystem>
#include <memory>
#include <vector>

#include <gar/api.h>

#include "utils/logging.hpp"

// TODO(gitbuda): Add simple implementation of transactional support.
// TODO(gitbuda): Since only one label per node is supported -> make a reserved property for other labels.

namespace memgraph::storage::custom_storage {

struct GARDatabaseConfig {
  struct PerDatabase {
    std::filesystem::path root{std::filesystem::temp_directory_path()};  // single database root directory
    std::shared_ptr<graphar::InfoVersion> version;
    std::string vertex_metadata_suffix{".vertex.yaml"};
    std::string edge_metadata_suffix{".edge.yaml"};
    std::filesystem::path vertex_folder_prefix{"vertex"};
    std::filesystem::path edge_folder_prefix{"edge"};
    int64_t vertex_chunk_size{1024};
    int64_t edge_chunk_size{1024};
    int64_t edge_src_chunk_size{1024};
    int64_t edge_dst_chunk_size{1024};
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

inline auto InitVertexType(const GARDatabaseConfig::GARVertexType &vertex_type) {
  auto vertex_info = graphar::CreateVertexInfo(vertex_type.label, vertex_type.base->vertex_chunk_size,
                                               vertex_type.properties, vertex_type.Prefix(), vertex_type.base->version);
  MG_ASSERT(!vertex_info->Dump().has_error());
  MG_ASSERT(vertex_info->Save(vertex_type.SavePath()).ok());
  return vertex_info;
}

inline auto InitVertexTypes(const GARDatabaseConfig &config) {
  std::vector<std::shared_ptr<graphar::VertexInfo>> vertex_infos;
  for (const auto &vertex_type : config.vertex_types) {
    auto vertex_info = InitVertexType(vertex_type);
    vertex_infos.push_back(vertex_info);
  }
  return vertex_infos;
}

inline auto InitEdgeType(const GARDatabaseConfig::GAREdgeType &edge_type) {
  auto edge_info = graphar::CreateEdgeInfo(
      edge_type.src_label, edge_type.edge_type, edge_type.dst_label, edge_type.base->edge_chunk_size,
      edge_type.base->edge_src_chunk_size, edge_type.base->edge_dst_chunk_size, edge_type.base->is_directed,
      edge_type.adjacent_lists, edge_type.properties, edge_type.Prefix(), edge_type.base->version);
  MG_ASSERT(!edge_info->Dump().has_error());
  MG_ASSERT(edge_info->Save(edge_type.SavePath()).ok());
  return edge_info;
}

inline auto InitEdgeTypes(const GARDatabaseConfig &config) {
  std::vector<std::shared_ptr<graphar::EdgeInfo>> edge_infos;
  for (const auto &edge_type : config.edge_types) {
    auto edge_info = InitEdgeType(edge_type);
    edge_infos.push_back(edge_info);
  }
  return edge_infos;
}

}  // namespace memgraph::storage::custom_storage
