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

#include "indices.hpp"

#include <algorithm>
#include <functional>
#include <limits>

#include "storage/v3/delta.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/vertex.hpp"
#include "utils/bound.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::storage::v3 {

namespace {

/// Traverses deltas visible from transaction with start timestamp greater than
/// the provided timestamp, and calls the provided callback function for each
/// delta. If the callback ever returns true, traversal is stopped and the
/// function returns true. Otherwise, the function returns false.
template <typename TCallback>
bool AnyVersionSatisfiesPredicate(uint64_t timestamp, const Delta *delta, const TCallback &predicate) {
  while (delta != nullptr) {
    const auto commit_info = *delta->commit_info;
    // This is a committed change that we see so we shouldn't undo it.
    if (commit_info.is_locally_committed && commit_info.start_or_commit_timestamp.logical_id < timestamp) {
      break;
    }
    if (predicate(*delta)) {
      return true;
    }
    // Move to the next delta.
    delta = delta->next;
  }
  return false;
}

/// Helper function for label index garbage collection. Returns true if there's
/// a reachable version of the vertex that has the given label.
bool AnyVersionHasLabel(const Vertex &vertex, LabelId label, uint64_t timestamp) {
  bool has_label{false};
  bool deleted{false};
  const Delta *delta{nullptr};
  {
    has_label = utils::Contains(vertex.second.labels, label);
    deleted = vertex.second.deleted;
    delta = vertex.second.delta;
  }
  if (!deleted && has_label) {
    return true;
  }
  return AnyVersionSatisfiesPredicate(timestamp, delta, [&has_label, &deleted, label](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_LABEL:
        if (delta.label == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
        break;
      case Delta::Action::REMOVE_LABEL:
        if (delta.label == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      case Delta::Action::RECREATE_OBJECT: {
        MG_ASSERT(deleted, "Invalid database state!");
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_OBJECT: {
        MG_ASSERT(!deleted, "Invalid database state!");
        deleted = true;
        break;
      }
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
    return !deleted && has_label;
  });
}

/// Helper function for label-property index garbage collection. Returns true if
/// there's a reachable version of the vertex that has the given label and
/// property value.
bool AnyVersionHasLabelProperty(const Vertex &vertex, LabelId label, PropertyId key, const PropertyValue &value,
                                uint64_t timestamp) {
  bool has_label{false};
  bool current_value_equal_to_value = value.IsNull();
  bool deleted{false};
  const Delta *delta{nullptr};
  {
    has_label = utils::Contains(vertex.second.labels, label);
    current_value_equal_to_value = vertex.second.properties.IsPropertyEqual(key, value);
    deleted = vertex.second.deleted;
    delta = vertex.second.delta;
  }

  if (!deleted && has_label && current_value_equal_to_value) {
    return true;
  }

  return AnyVersionSatisfiesPredicate(
      timestamp, delta, [&has_label, &current_value_equal_to_value, &deleted, label, key, &value](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_LABEL:
            if (delta.label == label) {
              MG_ASSERT(!has_label, "Invalid database state!");
              has_label = true;
            }
            break;
          case Delta::Action::REMOVE_LABEL:
            if (delta.label == label) {
              MG_ASSERT(has_label, "Invalid database state!");
              has_label = false;
            }
            break;
          case Delta::Action::SET_PROPERTY:
            if (delta.property.key == key) {
              current_value_equal_to_value = delta.property.value == value;
            }
            break;
          case Delta::Action::RECREATE_OBJECT: {
            MG_ASSERT(deleted, "Invalid database state!");
            deleted = false;
            break;
          }
          case Delta::Action::DELETE_OBJECT: {
            MG_ASSERT(!deleted, "Invalid database state!");
            deleted = true;
            break;
          }
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
        return !deleted && has_label && current_value_equal_to_value;
      });
}

// Helper function for iterating through label index. Returns true if this
// transaction can see the given vertex, and the visible version has the given
// label.
bool CurrentVersionHasLabel(const Vertex &vertex, LabelId label, Transaction *transaction, View view) {
  bool deleted{false};
  bool has_label{false};
  const Delta *delta{nullptr};
  {
    deleted = vertex.second.deleted;
    has_label = utils::Contains(vertex.second.labels, label);
    delta = vertex.second.delta;
  }
  ApplyDeltasForRead(transaction, delta, view, [&deleted, &has_label, label](const Delta &delta) {
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
        MG_ASSERT(!deleted, "Invalid database state!");
        deleted = true;
        break;
      }
      case Delta::Action::RECREATE_OBJECT: {
        MG_ASSERT(deleted, "Invalid database state!");
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
  return !deleted && has_label;
}

// Helper function for iterating through label-property index. Returns true if
// this transaction can see the given vertex, and the visible version has the
// given label and property.
bool CurrentVersionHasLabelProperty(const Vertex &vertex, LabelId label, PropertyId key, const PropertyValue &value,
                                    Transaction *transaction, View view) {
  bool deleted{false};
  bool has_label{false};
  bool current_value_equal_to_value = value.IsNull();
  const Delta *delta{nullptr};
  {
    deleted = vertex.second.deleted;
    has_label = utils::Contains(vertex.second.labels, label);
    current_value_equal_to_value = vertex.second.properties.IsPropertyEqual(key, value);
    delta = vertex.second.delta;
  }
  ApplyDeltasForRead(transaction, delta, view,
                     [&deleted, &has_label, &current_value_equal_to_value, key, label, &value](const Delta &delta) {
                       switch (delta.action) {
                         case Delta::Action::SET_PROPERTY: {
                           if (delta.property.key == key) {
                             current_value_equal_to_value = delta.property.value == value;
                           }
                           break;
                         }
                         case Delta::Action::DELETE_OBJECT: {
                           MG_ASSERT(!deleted, "Invalid database state!");
                           deleted = true;
                           break;
                         }
                         case Delta::Action::RECREATE_OBJECT: {
                           MG_ASSERT(deleted, "Invalid database state!");
                           deleted = false;
                           break;
                         }
                         case Delta::Action::ADD_LABEL:
                           if (delta.label == label) {
                             MG_ASSERT(!has_label, "Invalid database state!");
                             has_label = true;
                           }
                           break;
                         case Delta::Action::REMOVE_LABEL:
                           if (delta.label == label) {
                             MG_ASSERT(has_label, "Invalid database state!");
                             has_label = false;
                           }
                           break;
                         case Delta::Action::ADD_IN_EDGE:
                         case Delta::Action::ADD_OUT_EDGE:
                         case Delta::Action::REMOVE_IN_EDGE:
                         case Delta::Action::REMOVE_OUT_EDGE:
                           break;
                       }
                     });
  return !deleted && has_label && current_value_equal_to_value;
}

}  // namespace

void LabelIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  auto it = index_.find(label);
  if (it == index_.end()) return;
  it->second.insert(Entry{vertex, tx.start_timestamp.logical_id});
}

bool LabelIndex::CreateIndex(LabelId label, VertexContainer &vertices) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  auto [it, emplaced] = index_.emplace(std::piecewise_construct, std::forward_as_tuple(label), std::forward_as_tuple());
  if (!emplaced) {
    // Index already exists.
    return false;
  }
  try {
    for (auto &vertex : vertices) {
      if (vertex.second.deleted || !VertexHasLabel(vertex, label)) {
        continue;
      }
      it->second.insert(Entry{&vertex, 0});
    }
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    index_.erase(it);
    throw;
  }
  return true;
}

std::vector<LabelId> LabelIndex::ListIndices() const {
  std::vector<LabelId> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void LabelIndex::RemoveObsoleteEntries(const uint64_t clean_up_before_timestamp) {
  for (auto &label_storage : index_) {
    auto &vertices_acc = label_storage.second;
    for (auto it = vertices_acc.begin(); it != vertices_acc.end();) {
      auto next_it = it;
      ++next_it;

      if (it->timestamp >= clean_up_before_timestamp) {
        it = next_it;
        continue;
      }

      if ((next_it != vertices_acc.end() && it->vertex == next_it->vertex) ||
          !AnyVersionHasLabel(*it->vertex, label_storage.first, clean_up_before_timestamp)) {
        vertices_acc.erase(*it);
      }

      it = next_it;
    }
  }
}

LabelIndex::Iterable::Iterator::Iterator(Iterable *self, LabelIndexContainer::iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr, self_->config_, *self_->vertex_validator_),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

LabelIndex::Iterable::Iterator &LabelIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void LabelIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_container_->end(); ++index_iterator_) {
    if (index_iterator_->vertex == current_vertex_) {
      continue;
    }
    if (CurrentVersionHasLabel(*index_iterator_->vertex, self_->label_, self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ = VertexAccessor{current_vertex_, self_->transaction_, self_->indices_, self_->config_,
                                                *self_->vertex_validator_};
      break;
    }
  }
}

LabelIndex::Iterable::Iterable(LabelIndexContainer &index_container, LabelId label, View view, Transaction *transaction,
                               Indices *indices, Config::Items config, const VertexValidator &vertex_validator)
    : index_container_(&index_container),
      label_(label),
      view_(view),
      transaction_(transaction),
      indices_(indices),
      config_(config),
      vertex_validator_(&vertex_validator) {}

bool LabelPropertyIndex::Entry::operator<(const Entry &rhs) const {
  if (value < rhs.value) {
    return true;
  }
  if (rhs.value < value) {
    return false;
  }
  return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
}

bool LabelPropertyIndex::Entry::operator==(const Entry &rhs) const {
  return value == rhs.value && vertex == rhs.vertex && timestamp == rhs.timestamp;
}

bool LabelPropertyIndex::Entry::operator<(const PropertyValue &rhs) const { return value < rhs; }

bool LabelPropertyIndex::Entry::operator==(const PropertyValue &rhs) const { return value == rhs; }

void LabelPropertyIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  for (auto &[label_prop, index] : index_) {
    if (label_prop.first != label) {
      continue;
    }
    auto prop_value = vertex->second.properties.GetProperty(label_prop.second);
    if (!prop_value.IsNull()) {
      index.emplace(Entry{prop_value, vertex, tx.start_timestamp.logical_id});
    }
  }
}

void LabelPropertyIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                             const Transaction &tx) {
  if (value.IsNull()) {
    return;
  }
  for (auto &[label_prop, index] : index_) {
    if (label_prop.second != property) {
      continue;
    }
    if (VertexHasLabel(*vertex, label_prop.first)) {
      index.emplace(Entry{value, vertex, tx.start_timestamp.logical_id});
    }
  }
}

bool LabelPropertyIndex::CreateIndex(LabelId label, PropertyId property, VertexContainer &vertices) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  auto [it, emplaced] =
      index_.emplace(std::piecewise_construct, std::forward_as_tuple(label, property), std::forward_as_tuple());
  if (!emplaced) {
    // Index already exists.
    return false;
  }
  try {
    for (auto &vertex : vertices) {
      if (vertex.second.deleted || !VertexHasLabel(vertex, label)) {
        continue;
      }
      auto value = vertex.second.properties.GetProperty(property);
      if (value.IsNull()) {
        continue;
      }
      it->second.emplace(Entry{value, &vertex, 0});
    }
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    index_.erase(it);
    throw;
  }
  return true;
}

std::vector<std::pair<LabelId, PropertyId>> LabelPropertyIndex::ListIndices() const {
  std::vector<std::pair<LabelId, PropertyId>> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void LabelPropertyIndex::RemoveObsoleteEntries(const uint64_t clean_up_before_timestamp) {
  for (auto &[label_property, index] : index_) {
    for (auto it = index.begin(); it != index.end();) {
      auto next_it = it;
      ++next_it;

      if (it->timestamp >= clean_up_before_timestamp) {
        it = next_it;
        continue;
      }

      if ((next_it != index.end() && it->vertex == next_it->vertex && it->value == next_it->value) ||
          !AnyVersionHasLabelProperty(*it->vertex, label_property.first, label_property.second, it->value,
                                      clean_up_before_timestamp)) {
        index.erase(it);
      }
      it = next_it;
    }
  }
}

LabelPropertyIndex::Iterable::Iterator::Iterator(Iterable *self, LabelPropertyIndexContainer::iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr, self_->config_, *self_->vertex_validator_),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

LabelPropertyIndex::Iterable::Iterator &LabelPropertyIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void LabelPropertyIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_container_->end(); ++index_iterator_) {
    if (index_iterator_->vertex == current_vertex_) {
      continue;
    }

    if (self_->lower_bound_) {
      if (index_iterator_->value < self_->lower_bound_->value()) {
        continue;
      }
      if (!self_->lower_bound_->IsInclusive() && index_iterator_->value == self_->lower_bound_->value()) {
        continue;
      }
    }
    if (self_->upper_bound_) {
      if (self_->upper_bound_->value() < index_iterator_->value) {
        index_iterator_ = self_->index_container_->end();
        break;
      }
      if (!self_->upper_bound_->IsInclusive() && index_iterator_->value == self_->upper_bound_->value()) {
        index_iterator_ = self_->index_container_->end();
        break;
      }
    }

    if (CurrentVersionHasLabelProperty(*index_iterator_->vertex, self_->label_, self_->property_,
                                       index_iterator_->value, self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ = VertexAccessor(current_vertex_, self_->transaction_, self_->indices_, self_->config_,
                                                *self_->vertex_validator_);
      break;
    }
  }
}

// These constants represent the smallest possible value of each type that is
// contained in a `PropertyValue`. Note that numbers (integers and doubles) are
// treated as the same "type" in `PropertyValue`.
const PropertyValue kSmallestBool = PropertyValue(false);
static_assert(-std::numeric_limits<double>::infinity() < static_cast<double>(std::numeric_limits<int64_t>::min()));
const PropertyValue kSmallestNumber = PropertyValue(-std::numeric_limits<double>::infinity());
const PropertyValue kSmallestString = PropertyValue("");
const PropertyValue kSmallestList = PropertyValue(std::vector<PropertyValue>());
const PropertyValue kSmallestMap = PropertyValue(std::map<std::string, PropertyValue>());
const PropertyValue kSmallestTemporalData =
    PropertyValue(TemporalData{static_cast<TemporalType>(0), std::numeric_limits<int64_t>::min()});

LabelPropertyIndex::Iterable::Iterable(LabelPropertyIndexContainer &index_container, LabelId label, PropertyId property,
                                       const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                       const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                       Transaction *transaction, Indices *indices, Config::Items config,
                                       const VertexValidator &vertex_validator)
    : index_container_(&index_container),
      label_(label),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      view_(view),
      transaction_(transaction),
      indices_(indices),
      config_(config),
      vertex_validator_(&vertex_validator) {
  // We have to fix the bounds that the user provided to us. If the user
  // provided only one bound we should make sure that only values of that type
  // are returned by the iterator. We ensure this by supplying either an
  // inclusive lower bound of the same type, or an exclusive upper bound of the
  // following type. If neither bound is set we yield all items in the index.

  // First we statically verify that our assumptions about the `PropertyValue`
  // type ordering holds.
  static_assert(PropertyValue::Type::Bool < PropertyValue::Type::Int);
  static_assert(PropertyValue::Type::Int < PropertyValue::Type::Double);
  static_assert(PropertyValue::Type::Double < PropertyValue::Type::String);
  static_assert(PropertyValue::Type::String < PropertyValue::Type::List);
  static_assert(PropertyValue::Type::List < PropertyValue::Type::Map);

  // Remove any bounds that are set to `Null` because that isn't a valid value.
  if (lower_bound_ && lower_bound_->value().IsNull()) {
    lower_bound_ = std::nullopt;
  }
  if (upper_bound_ && upper_bound_->value().IsNull()) {
    upper_bound_ = std::nullopt;
  }

  // Check whether the bounds are of comparable types if both are supplied.
  if (lower_bound_ && upper_bound_ &&
      !PropertyValue::AreComparableTypes(lower_bound_->value().type(), upper_bound_->value().type())) {
    bounds_valid_ = false;
    return;
  }

  // Set missing bounds.
  if (lower_bound_ && !upper_bound_) {
    // Here we need to supply an upper bound. The upper bound is set to an
    // exclusive lower bound of the following type.
    switch (lower_bound_->value().type()) {
      case PropertyValue::Type::Null:
        // This shouldn't happen because of the nullopt-ing above.
        LOG_FATAL("Invalid database state!");
        break;
      case PropertyValue::Type::Bool:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestNumber);
        break;
      case PropertyValue::Type::Int:
      case PropertyValue::Type::Double:
        // Both integers and doubles are treated as the same type in
        // `PropertyValue` and they are interleaved when sorted.
        upper_bound_ = utils::MakeBoundExclusive(kSmallestString);
        break;
      case PropertyValue::Type::String:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestList);
        break;
      case PropertyValue::Type::List:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestMap);
        break;
      case PropertyValue::Type::Map:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestTemporalData);
        break;
      case PropertyValue::Type::TemporalData:
        // This is the last type in the order so we leave the upper bound empty.
        break;
    }
  }
  if (upper_bound_ && !lower_bound_) {
    // Here we need to supply a lower bound. The lower bound is set to an
    // inclusive lower bound of the current type.
    switch (upper_bound_->value().type()) {
      case PropertyValue::Type::Null:
        // This shouldn't happen because of the nullopt-ing above.
        LOG_FATAL("Invalid database state!");
        break;
      case PropertyValue::Type::Bool:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestBool);
        break;
      case PropertyValue::Type::Int:
      case PropertyValue::Type::Double:
        // Both integers and doubles are treated as the same type in
        // `PropertyValue` and they are interleaved when sorted.
        lower_bound_ = utils::MakeBoundInclusive(kSmallestNumber);
        break;
      case PropertyValue::Type::String:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestString);
        break;
      case PropertyValue::Type::List:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestList);
        break;
      case PropertyValue::Type::Map:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestMap);
        break;
      case PropertyValue::Type::TemporalData:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestTemporalData);
        break;
    }
  }
}

LabelPropertyIndex::Iterable::Iterator LabelPropertyIndex::Iterable::begin() {
  // If the bounds are set and don't have comparable types we don't yield any
  // items from the index.
  if (!bounds_valid_) {
    return {this, index_container_->end()};
  }
  if (lower_bound_) {
    return {this, std::ranges::lower_bound(*index_container_, lower_bound_->value(), std::less{}, &Entry::value)};
  }
  return {this, index_container_->begin()};
}

LabelPropertyIndex::Iterable::Iterator LabelPropertyIndex::Iterable::end() { return {this, index_container_->end()}; }

int64_t LabelPropertyIndex::VertexCount(LabelId label, PropertyId property, const PropertyValue &value) const {
  auto it = index_.find({label, property});
  MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(), property.AsUint());
  MG_ASSERT(!value.IsNull(), "Null is not supported!");

  // TODO(jbajic) This can be improved by exiting early
  auto start_it = std::ranges::lower_bound(it->second, value, std::less{}, &Entry::value);
  return static_cast<int64_t>(
      std::ranges::count_if(start_it, it->second.end(), [&value](const auto &elem) { return elem.value == value; }));
}

int64_t LabelPropertyIndex::VertexCount(LabelId label, PropertyId property,
                                        const std::optional<utils::Bound<PropertyValue>> &lower,
                                        const std::optional<utils::Bound<PropertyValue>> &upper) const {
  auto it = index_.find({label, property});
  MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(), property.AsUint());
  const auto lower_it = std::invoke(
      [&index = it->second](const auto value, const auto def) {
        if (value) {
          if (value->IsInclusive()) {
            return std::ranges::lower_bound(index, value->value(), std::less{}, &Entry::value);
          }
          return std::ranges::upper_bound(index, value->value(), std::less{}, &Entry::value);
        }
        return def;
      },
      lower, it->second.begin());
  const auto upper_it = std::invoke(
      [&index = it->second](const auto value, const auto def) {
        if (value) {
          if (value->IsInclusive()) {
            return std::ranges::upper_bound(index, value->value(), std::less{}, &Entry::value);
          }
          return std::ranges::lower_bound(index, value->value(), std::less{}, &Entry::value);
        }
        return def;
      },
      upper, it->second.end());
  return static_cast<int64_t>(std::distance(lower_it, upper_it));
}

void RemoveObsoleteEntries(Indices *indices, const uint64_t clean_up_before_timestamp) {
  indices->label_index.RemoveObsoleteEntries(clean_up_before_timestamp);
  indices->label_property_index.RemoveObsoleteEntries(clean_up_before_timestamp);
}

void UpdateOnAddLabel(Indices *indices, LabelId label, Vertex *vertex, const Transaction &tx) {
  indices->label_index.UpdateOnAddLabel(label, vertex, tx);
  indices->label_property_index.UpdateOnAddLabel(label, vertex, tx);
}

void UpdateOnSetProperty(Indices *indices, PropertyId property, const PropertyValue &value, Vertex *vertex,
                         const Transaction &tx) {
  indices->label_property_index.UpdateOnSetProperty(property, value, vertex, tx);
}

}  // namespace memgraph::storage::v3
