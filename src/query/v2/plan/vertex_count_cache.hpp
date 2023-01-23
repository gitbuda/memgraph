// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <iterator>
#include <optional>

#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/plan/preprocess.hpp"
#include "query/v2/request_router.hpp"
#include "storage/v3/conversions.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"

namespace memgraph::query::v2::plan {

/// A stand in class for `TDbAccessor` which provides memoized calls to
/// `VerticesCount`.
template <class TDbAccessor>
class VertexCountCache {
 public:
  explicit VertexCountCache(TDbAccessor *request_router) : request_router_{request_router} {}

  auto NameToLabel(const std::string &name) { return request_router_->NameToLabel(name); }
  auto NameToProperty(const std::string &name) { return request_router_->NameToProperty(name); }
  auto NameToEdgeType(const std::string &name) { return request_router_->NameToEdgeType(name); }

  int64_t VerticesCount() { return 1; }

  int64_t VerticesCount(storage::v3::LabelId /*label*/) { return 1; }

  int64_t VerticesCount(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/) { return 1; }

  int64_t VerticesCount(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/,
                        const storage::v3::PropertyValue & /*value*/) {
    return 1;
  }

  int64_t VerticesCount(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> & /*lower*/,
                        const std::optional<utils::Bound<storage::v3::PropertyValue>> & /*upper*/) {
    return 1;
  }

  bool LabelIndexExists(storage::v3::LabelId label) { return PrimaryLabelExists(label); }

  bool PrimaryLabelExists(storage::v3::LabelId label) { return request_router_->IsPrimaryLabel(label); }

  bool LabelPropertyIndexExists(storage::v3::LabelId /*label*/, storage::v3::PropertyId /*property*/) { return false; }

  std::vector<memgraph::storage::v3::SchemaProperty> GetSchemaForLabel(storage::v3::LabelId label) {
    return request_router_->GetSchemaForLabel(label);
  }

  RequestRouterInterface *request_router_;
};

template <class TDbAccessor>
auto MakeVertexCountCache(TDbAccessor *db) {
  return VertexCountCache<TDbAccessor>(db);
}

}  // namespace memgraph::query::v2::plan
