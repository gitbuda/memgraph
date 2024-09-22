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
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

// TODO(gitbuda): To create edge, GAR internal vertex id for a given label is required -> calculate of propagate.
// TODO(gitbuda): What is the right type for IDs?
// TODO(gitbuda): How to safely create all PMR values with minimal code and maximal flexibility? -> PMR has overhead.
// NOTE: PMR reasoning -> this should be allocator aware because after import, all that could be deleted.
//   * C++Weekly#235 -> https://www.youtube.com/watch?v=vXJ1dwJ9QkI
//   * C++Weekly#236 -> https://www.youtube.com/watch?v=2LAsqp7UrNs
//   * --> take a look at tests/manual/pmr.cpp how to make an allocator aware type.

namespace memgraph::storage::custom_storage {

struct Vertex {
  // This is here because of the hybrid-schema option (having different type of IDs)
  PropertyValue id;
  std::vector<LabelId> labels;  // NOTE: GAR only supports one label per vertex!
  // Consider replacing map with PropertyStore because it's more efficient.
  // NOTE: map is below just because that's comes from the query engine (example purposes).
  std::map<PropertyId, PropertyValue> properties;
};

struct Edge {
  PropertyValue id;
  PropertyValue src_id;
  PropertyValue dst_id;
  EdgeTypeId edge_type;
  // Consider replacing map with PropertyStore because it's more efficient.
  std::unordered_map<PropertyId, PropertyValue> properties;
};

}  // namespace memgraph::storage::custom_storage
