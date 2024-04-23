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

#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

// TODO(gitbuda): What is the right type for IDs?
// TODO(gitbuda): How to safely create all PMR values with minimal code and maximal flexibility?

namespace memgraph::storage::custom_storage {

using PropertyValue = std::variant<int64_t, std::pmr::string>;

struct Vertex {
  PropertyValue id;
  std::pmr::vector<std::pmr::string> labels;
  std::pmr::unordered_map<std::string, PropertyValue> properties;
};

struct Edge {
  PropertyValue src_id;
  PropertyValue dst_id;
  std::pmr::string edge_type;
  std::pmr::unordered_map<std::string, PropertyValue> properties;
};

}  // namespace memgraph::storage::custom_storage
