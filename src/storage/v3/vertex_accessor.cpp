// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v3/vertex_accessor.hpp"

#include <cstddef>
#include <memory>

#include "storage/v3/conversions.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage::v3 {

namespace detail {
namespace {
std::pair<bool, bool> IsVisible(Vertex *vertex, Transaction *transaction, View view) {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    deleted = vertex->second.deleted;
    delta = vertex->second.delta;
  }
  ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
    }
  });

  return {exists, deleted};
}
}  // namespace
}  // namespace detail

std::optional<VertexAccessor> VertexAccessor::Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                                     Config::Items config, const VertexValidator &vertex_validator,
                                                     View view) {
  if (const auto [exists, deleted] = detail::IsVisible(vertex, transaction, view); !exists || deleted) {
    return std::nullopt;
  }

  return VertexAccessor{vertex, transaction, indices, config, vertex_validator};
}

bool VertexAccessor::IsVisible(View view) const {
  const auto [exists, deleted] = detail::IsVisible(vertex_, transaction_, view);
  return exists && (for_deleted_ || !deleted);
}

ShardResult<bool> VertexAccessor::AddLabel(LabelId label) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  if (!PrepareForWrite(transaction_, vertex_)) return SHARD_ERROR(ErrorCode::SERIALIZATION_ERROR);

  if (vertex_->second.deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);

  if (std::find(vertex_->second.labels.begin(), vertex_->second.labels.end(), label) != vertex_->second.labels.end())
    return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::RemoveLabelTag(), label);

  vertex_->second.labels.push_back(label);

  UpdateOnAddLabel(indices_, label, vertex_, *transaction_);

  return true;
}

ShardResult<bool> VertexAccessor::AddLabelAndValidate(LabelId label) {
  if (const auto maybe_violation_error = vertex_validator_->ValidateAddLabel(label); maybe_violation_error.HasError()) {
    return {maybe_violation_error.GetError()};
  }
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  if (!PrepareForWrite(transaction_, vertex_)) return SHARD_ERROR(ErrorCode::SERIALIZATION_ERROR);

  if (vertex_->second.deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);

  if (std::find(vertex_->second.labels.begin(), vertex_->second.labels.end(), label) != vertex_->second.labels.end())
    return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::RemoveLabelTag(), label);

  vertex_->second.labels.push_back(label);

  UpdateOnAddLabel(indices_, label, vertex_, *transaction_);

  return true;
}

ShardResult<bool> VertexAccessor::RemoveLabel(LabelId label) {
  if (!PrepareForWrite(transaction_, vertex_)) return SHARD_ERROR(ErrorCode::SERIALIZATION_ERROR);

  if (vertex_->second.deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);

  auto it = std::find(vertex_->second.labels.begin(), vertex_->second.labels.end(), label);
  if (it == vertex_->second.labels.end()) return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::AddLabelTag(), label);

  std::swap(*it, *vertex_->second.labels.rbegin());
  vertex_->second.labels.pop_back();
  return true;
}

ShardResult<bool> VertexAccessor::RemoveLabelAndValidate(LabelId label) {
  if (const auto maybe_violation_error = vertex_validator_->ValidateRemoveLabel(label);
      maybe_violation_error.HasError()) {
    return {maybe_violation_error.GetError()};
  }

  if (!PrepareForWrite(transaction_, vertex_)) return SHARD_ERROR(ErrorCode::SERIALIZATION_ERROR);

  if (vertex_->second.deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);

  auto it = std::find(vertex_->second.labels.begin(), vertex_->second.labels.end(), label);
  if (it == vertex_->second.labels.end()) return false;

  CreateAndLinkDelta(transaction_, vertex_, Delta::AddLabelTag(), label);

  std::swap(*it, *vertex_->second.labels.rbegin());
  vertex_->second.labels.pop_back();
  return true;
}

ShardResult<bool> VertexAccessor::HasLabel(View view, LabelId label) const { return HasLabel(label, view); }

ShardResult<bool> VertexAccessor::HasLabel(LabelId label, View view) const {
  bool exists = true;
  bool deleted = false;
  bool has_label = false;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    has_label = label == vertex_validator_->primary_label_ || VertexHasLabel(*vertex_, label);
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &has_label, label](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::REMOVE_LABEL: {
        if (delta.label == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      }
      case Delta::Action::ADD_LABEL: {
        if (delta.label == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  if (!for_deleted_ && deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  return has_label;
}

ShardResult<LabelId> VertexAccessor::PrimaryLabel(const View view) const {
  if (const auto result = CheckVertexExistence(view); result.HasError()) {
    return result.GetError();
  }

  return vertex_validator_->primary_label_;
}

ShardResult<PrimaryKey> VertexAccessor::PrimaryKey(const View view) const {
  if (const auto result = CheckVertexExistence(view); result.HasError()) {
    return result.GetError();
  }
  return vertex_->first;
}

ShardResult<VertexId> VertexAccessor::Id(View view) const {
  if (const auto result = CheckVertexExistence(view); result.HasError()) {
    return result.GetError();
  }
  return VertexId{vertex_validator_->primary_label_, vertex_->first};
};

ShardResult<std::vector<LabelId>> VertexAccessor::Labels(View view) const {
  bool exists = true;
  bool deleted = false;
  std::vector<LabelId> labels;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    labels = vertex_->second.labels;
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &labels](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::REMOVE_LABEL: {
        // Remove the label because we don't see the addition.
        auto it = std::find(labels.begin(), labels.end(), delta.label);
        MG_ASSERT(it != labels.end(), "Invalid database state!");
        std::swap(*it, *labels.rbegin());
        labels.pop_back();
        break;
      }
      case Delta::Action::ADD_LABEL: {
        // Add the label because we don't see the removal.
        auto it = std::find(labels.begin(), labels.end(), delta.label);
        MG_ASSERT(it == labels.end(), "Invalid database state!");
        labels.push_back(delta.label);
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  if (!for_deleted_ && deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  return std::move(labels);
}

ShardResult<PropertyValue> VertexAccessor::SetProperty(PropertyId property, const PropertyValue &value) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  if (!PrepareForWrite(transaction_, vertex_)) return SHARD_ERROR(ErrorCode::SERIALIZATION_ERROR);

  if (vertex_->second.deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);

  auto current_value = vertex_->second.properties.GetProperty(property);
  // We could skip setting the value if the previous one is the same to the new
  // one. This would save some memory as a delta would not be created as well as
  // avoid copying the value. The reason we are not doing that is because the
  // current code always follows the logical pattern of "create a delta" and
  // "modify in-place". Additionally, the created delta will make other
  // transactions get a SERIALIZATION_ERROR.
  CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property, current_value);
  vertex_->second.properties.SetProperty(property, value);

  UpdateOnSetProperty(indices_, property, value, vertex_, *transaction_);

  return std::move(current_value);
}

ShardResult<void> VertexAccessor::CheckVertexExistence(View view) const {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) {
    return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  }
  if (!for_deleted_ && deleted) {
    return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  }
  return {};
}

ShardResult<PropertyValue> VertexAccessor::SetPropertyAndValidate(PropertyId property, const PropertyValue &value) {
  if (auto maybe_violation_error = vertex_validator_->ValidatePropertyUpdate(property);
      maybe_violation_error.HasError()) {
    return {maybe_violation_error.GetError()};
  }
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;

  if (!PrepareForWrite(transaction_, vertex_)) {
    return SHARD_ERROR(ErrorCode::SERIALIZATION_ERROR);
  }

  if (vertex_->second.deleted) {
    return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  }

  auto current_value = vertex_->second.properties.GetProperty(property);
  // We could skip setting the value if the previous one is the same to the new
  // one. This would save some memory as a delta would not be created as well as
  // avoid copying the value. The reason we are not doing that is because the
  // current code always follows the logical pattern of "create a delta" and
  // "modify in-place". Additionally, the created delta will make other
  // transactions get a SERIALIZATION_ERROR.
  CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property, current_value);
  vertex_->second.properties.SetProperty(property, value);

  UpdateOnSetProperty(indices_, property, value, vertex_, *transaction_);

  return std::move(current_value);
}

ShardResult<std::map<PropertyId, PropertyValue>> VertexAccessor::ClearProperties() {
  if (!PrepareForWrite(transaction_, vertex_)) return SHARD_ERROR(ErrorCode::SERIALIZATION_ERROR);

  if (vertex_->second.deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);

  auto properties = vertex_->second.properties.Properties();
  for (const auto &property : properties) {
    CreateAndLinkDelta(transaction_, vertex_, Delta::SetPropertyTag(), property.first, property.second);
    UpdateOnSetProperty(indices_, property.first, PropertyValue(), vertex_, *transaction_);
  }

  vertex_->second.properties.ClearProperties();

  return std::move(properties);
}

ShardResult<PropertyValue> VertexAccessor::GetProperty(View view, PropertyId property) const {
  return GetProperty(property, view).GetValue();
}

PropertyValue VertexAccessor::GetPropertyValue(PropertyId property, View view) const {
  PropertyValue value;

  const auto primary_label = PrimaryLabel(view);
  if (primary_label.HasError()) {
    return value;
  }
  const auto *schema = vertex_validator_->schema_validator->GetSchema(*primary_label);
  if (!schema) {
    return value;
  }
  // Find PropertyId index in keystore
  for (size_t property_index{0}; property_index < schema->second.size(); ++property_index) {
    if (schema->second[property_index].property_id == property) {
      return vertex_->first[property_index];
    }
  }

  return value = vertex_->second.properties.GetProperty(property);
}

ShardResult<PropertyValue> VertexAccessor::GetProperty(PropertyId property, View view) const {
  bool exists = true;
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    value = GetPropertyValue(property, view);
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &value, property](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::SET_PROPERTY: {
        if (delta.property.key == property) {
          value = delta.property.value;
        }
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) {
    return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  }
  if (!for_deleted_ && deleted) {
    return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  }
  return std::move(value);
}

ShardResult<std::map<PropertyId, PropertyValue>> VertexAccessor::Properties(View view) const {
  bool exists = true;
  bool deleted = false;
  std::map<PropertyId, PropertyValue> properties;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    // TODO(antaljanosbenjamin): Should this also return the primary key?
    properties = vertex_->second.properties.Properties();
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &properties](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::SET_PROPERTY: {
        auto it = properties.find(delta.property.key);
        if (it != properties.end()) {
          if (delta.property.value.IsNull()) {
            // remove the property
            properties.erase(it);
          } else {
            // set the value
            it->second = delta.property.value;
          }
        } else if (!delta.property.value.IsNull()) {
          properties.emplace(delta.property.key, delta.property.value);
        }
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  if (!for_deleted_ && deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  return std::move(properties);
}

ShardResult<std::vector<EdgeAccessor>> VertexAccessor::InEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                               const VertexId *destination_id) const {
  bool exists = true;
  bool deleted = false;
  std::vector<VertexData::EdgeLink> in_edges;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    if (edge_types.empty() && nullptr == destination_id) {
      in_edges = vertex_->second.in_edges;
    } else {
      for (const auto &item : vertex_->second.in_edges) {
        const auto &[edge_type, from_vertex, edge] = item;
        if (nullptr != destination_id && from_vertex != *destination_id) {
          continue;
        };
        if (!edge_types.empty() && std::find(edge_types.begin(), edge_types.end(), edge_type) == edge_types.end())
          continue;
        in_edges.push_back(item);
      }
    }
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(
      transaction_, delta, view, [&exists, &deleted, &in_edges, &edge_types, destination_id](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_IN_EDGE: {
            if (nullptr != destination_id && delta.vertex_edge.vertex_id != *destination_id) break;
            if (!edge_types.empty() &&
                std::find(edge_types.begin(), edge_types.end(), delta.vertex_edge.edge_type) == edge_types.end())
              break;
            // Add the edge because we don't see the removal.
            VertexData::EdgeLink link{delta.vertex_edge.edge_type, delta.vertex_edge.vertex_id, delta.vertex_edge.edge};
            auto it = std::find(in_edges.begin(), in_edges.end(), link);
            MG_ASSERT(it == in_edges.end(), "Invalid database state!");
            in_edges.push_back(link);
            break;
          }
          case Delta::Action::REMOVE_IN_EDGE: {
            if (nullptr != destination_id && delta.vertex_edge.vertex_id != *destination_id) break;
            if (!edge_types.empty() &&
                std::find(edge_types.begin(), edge_types.end(), delta.vertex_edge.edge_type) == edge_types.end())
              break;
            // Remove the label because we don't see the addition.
            VertexData::EdgeLink link{delta.vertex_edge.edge_type, delta.vertex_edge.vertex_id, delta.vertex_edge.edge};
            auto it = std::find(in_edges.begin(), in_edges.end(), link);
            MG_ASSERT(it != in_edges.end(), "Invalid database state!");
            std::swap(*it, *in_edges.rbegin());
            in_edges.pop_back();
            break;
          }
          case Delta::Action::DELETE_OBJECT: {
            exists = false;
            break;
          }
          case Delta::Action::RECREATE_OBJECT: {
            deleted = false;
            break;
          }
          case Delta::Action::ADD_LABEL:
          case Delta::Action::REMOVE_LABEL:
          case Delta::Action::SET_PROPERTY:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
      });
  if (!exists) return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  if (deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  std::vector<EdgeAccessor> ret;
  if (in_edges.empty()) {
    return ret;
  }
  ret.reserve(in_edges.size());
  const auto id = VertexId{vertex_validator_->primary_label_, vertex_->first};
  for (const auto &item : in_edges) {
    const auto &[edge_type, from_vertex, edge] = item;
    ret.emplace_back(edge, edge_type, from_vertex, id, transaction_, indices_, config_);
  }
  return ret;
}

ShardResult<std::vector<EdgeAccessor>> VertexAccessor::OutEdges(View view, const std::vector<EdgeTypeId> &edge_types,
                                                                const VertexId *destination_id) const {
  bool exists = true;
  bool deleted = false;
  std::vector<VertexData::EdgeLink> out_edges;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    if (edge_types.empty() && nullptr == destination_id) {
      out_edges = vertex_->second.out_edges;
    } else {
      for (const auto &item : vertex_->second.out_edges) {
        const auto &[edge_type, to_vertex, edge] = item;
        if (nullptr != destination_id && to_vertex != *destination_id) continue;
        if (!edge_types.empty() && std::find(edge_types.begin(), edge_types.end(), edge_type) == edge_types.end())
          continue;
        out_edges.push_back(item);
      }
    }
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(
      transaction_, delta, view, [&exists, &deleted, &out_edges, &edge_types, destination_id](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_OUT_EDGE: {
            if (nullptr != destination_id && delta.vertex_edge.vertex_id != *destination_id) break;
            if (!edge_types.empty() &&
                std::find(edge_types.begin(), edge_types.end(), delta.vertex_edge.edge_type) == edge_types.end())
              break;
            // Add the edge because we don't see the removal.
            VertexData::EdgeLink link{delta.vertex_edge.edge_type, delta.vertex_edge.vertex_id, delta.vertex_edge.edge};
            auto it = std::find(out_edges.begin(), out_edges.end(), link);
            MG_ASSERT(it == out_edges.end(), "Invalid database state!");
            out_edges.push_back(link);
            break;
          }
          case Delta::Action::REMOVE_OUT_EDGE: {
            if (nullptr != destination_id && delta.vertex_edge.vertex_id != *destination_id) break;
            if (!edge_types.empty() &&
                std::find(edge_types.begin(), edge_types.end(), delta.vertex_edge.edge_type) == edge_types.end())
              break;
            // Remove the label because we don't see the addition.
            VertexData::EdgeLink link{delta.vertex_edge.edge_type, delta.vertex_edge.vertex_id, delta.vertex_edge.edge};
            auto it = std::find(out_edges.begin(), out_edges.end(), link);
            MG_ASSERT(it != out_edges.end(), "Invalid database state!");
            std::swap(*it, *out_edges.rbegin());
            out_edges.pop_back();
            break;
          }
          case Delta::Action::DELETE_OBJECT: {
            exists = false;
            break;
          }
          case Delta::Action::RECREATE_OBJECT: {
            deleted = false;
            break;
          }
          case Delta::Action::ADD_LABEL:
          case Delta::Action::REMOVE_LABEL:
          case Delta::Action::SET_PROPERTY:
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
            break;
        }
      });
  if (!exists) return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  if (deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  std::vector<EdgeAccessor> ret;
  if (out_edges.empty()) {
    return ret;
  }
  ret.reserve(out_edges.size());
  const auto id = VertexId{vertex_validator_->primary_label_, vertex_->first};
  for (const auto &item : out_edges) {
    const auto &[edge_type, to_vertex, edge] = item;
    ret.emplace_back(edge, edge_type, id, to_vertex, transaction_, indices_, config_);
  }
  return ret;
}

ShardResult<size_t> VertexAccessor::InDegree(View view) const {
  bool exists = true;
  bool deleted = false;
  size_t degree = 0;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    degree = vertex_->second.in_edges.size();
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &degree](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_IN_EDGE:
        ++degree;
        break;
      case Delta::Action::REMOVE_IN_EDGE:
        --degree;
        break;
      case Delta::Action::DELETE_OBJECT:
        exists = false;
        break;
      case Delta::Action::RECREATE_OBJECT:
        deleted = false;
        break;
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (!exists) return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  if (!for_deleted_ && deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  return degree;
}

ShardResult<size_t> VertexAccessor::OutDegree(View view) const {
  bool exists = true;
  bool deleted = false;
  size_t degree = 0;
  Delta *delta = nullptr;
  {
    deleted = vertex_->second.deleted;
    degree = vertex_->second.out_edges.size();
    delta = vertex_->second.delta;
  }
  ApplyDeltasForRead(transaction_, delta, view, [&exists, &deleted, &degree](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_OUT_EDGE:
        ++degree;
        break;
      case Delta::Action::REMOVE_OUT_EDGE:
        --degree;
        break;
      case Delta::Action::DELETE_OBJECT:
        exists = false;
        break;
      case Delta::Action::RECREATE_OBJECT:
        deleted = false;
        break;
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
        break;
    }
  });
  if (!exists) return SHARD_ERROR(ErrorCode::NONEXISTENT_OBJECT);
  if (!for_deleted_ && deleted) return SHARD_ERROR(ErrorCode::DELETED_OBJECT);
  return degree;
}

}  // namespace memgraph::storage::v3
