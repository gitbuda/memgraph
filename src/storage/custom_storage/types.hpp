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

#include <map>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

// TODO(gitbuda): To create edge, GAR internal vertex id for a given label is required -> calculate of propagate.
// TODO(gitbuda): What is the right type for IDs?
// TODO(gitbuda): How to safely create all PMR values with minimal code and maximal flexibility?

namespace memgraph::storage::custom_storage {

// NOTE: This should be allocator aware because after import, all that could be deleted.
//   * C++Weekly#235 -> https://www.youtube.com/watch?v=vXJ1dwJ9QkI
//   * C++Weekly#236 -> https://www.youtube.com/watch?v=2LAsqp7UrNs

// TODO(gitbuda): Make and test Vertex being allocator aware.
struct Vertex {
  // This is here because of the hybrid-schema option (having different type of IDs)
  memgraph::storage::PropertyValue id;
  std::vector<std::string> labels;
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> properties;
  // std::pmr::vector<std::pmr::string> labels;  // NOTE: GAR only supports one label per vertex!
  // std::pmr::unordered_map<std::string, PropertyValue> properties;
};

struct Edge {
  PropertyValue src_id;
  PropertyValue dst_id;
  std::pmr::string edge_type;
  std::pmr::unordered_map<std::string, PropertyValue> properties;
};

}  // namespace memgraph::storage::custom_storage
