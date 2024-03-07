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

#pragma once

#include <cstdint>
#include <optional>
#include <set>
#include <tuple>
#include <utility>

#include "storage/v3/config.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "utils/bound.hpp"
#include "utils/logging.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage::v3 {

struct Indices;

class LabelIndex {
  struct Entry {
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) const {
      return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
    }
    bool operator==(const Entry &rhs) const { return vertex == rhs.vertex && timestamp == rhs.timestamp; }
  };

  using IndexType = LabelId;

 public:
  using IndexContainer = std::set<Entry>;

  LabelIndex(Indices *indices, Config::Items config, const VertexValidator &vertex_validator)
      : indices_(indices), config_(config), vertex_validator_{&vertex_validator} {}

  LabelIndex(Indices *indices, Config::Items config, const VertexValidator &vertex_validator,
             std::map<LabelId, IndexContainer> &data)
      : index_{std::move(data)}, indices_(indices), config_(config), vertex_validator_{&vertex_validator} {}

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, VertexContainer &vertices);

  /// Returns false if there was no index to drop
  bool DropIndex(LabelId label) { return index_.erase(label) > 0; }

  bool IndexExists(LabelId label) const { return index_.find(label) != index_.end(); }

  std::vector<LabelId> ListIndices() const;

  void RemoveObsoleteEntries(uint64_t clean_up_before_timestamp);

  class Iterable {
   public:
    Iterable(IndexContainer &index_container, LabelId label, View view, Transaction *transaction, Indices *indices,
             Config::Items config, const VertexValidator &vertex_validator);

    class Iterator {
     public:
      Iterator(Iterable *self, IndexContainer::iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      IndexContainer::iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin() { return {this, index_container_->begin()}; }
    Iterator end() { return {this, index_container_->end()}; }

   private:
    IndexContainer *index_container_;
    LabelId label_;
    View view_;
    Transaction *transaction_;
    Indices *indices_;
    Config::Items config_;
    const VertexValidator *vertex_validator_;
  };

  /// Returns an self with vertices visible from the given transaction.
  Iterable Vertices(LabelId label, View view, Transaction *transaction) {
    auto it = index_.find(label);
    MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
    return {it->second, label, view, transaction, indices_, config_, *vertex_validator_};
  }

  int64_t ApproximateVertexCount(LabelId label) {
    auto it = index_.find(label);
    MG_ASSERT(it != index_.end(), "Index for label {} doesn't exist", label.AsUint());
    return static_cast<int64_t>(it->second.size());
  }

  void Clear() { index_.clear(); }

  std::map<IndexType, IndexContainer> SplitIndexEntries(const PrimaryKey &split_key) {
    std::map<IndexType, IndexContainer> cloned_indices;
    for (auto &[index_type_val, index] : index_) {
      auto entry_it = index.begin();
      auto &cloned_indices_container = cloned_indices[index_type_val];
      while (entry_it != index.end()) {
        // We need to save the next iterator since the current one will be
        // invalidated after extract
        auto next_entry_it = std::next(entry_it);
        if (entry_it->vertex->first > split_key) {
          [[maybe_unused]] const auto &[inserted_entry_it, inserted, node] =
              cloned_indices_container.insert(index.extract(entry_it));
          MG_ASSERT(inserted, "Failed to extract index entry!");
        }
        entry_it = next_entry_it;
      }
    }

    return cloned_indices;
  }

 private:
  std::map<LabelId, IndexContainer> index_;
  Indices *indices_;
  Config::Items config_;
  const VertexValidator *vertex_validator_;
};

class LabelPropertyIndex {
  struct Entry {
    PropertyValue value;
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;

    bool operator<(const PropertyValue &rhs) const;
    bool operator==(const PropertyValue &rhs) const;
  };
  using IndexType = std::pair<LabelId, PropertyId>;

 public:
  using IndexContainer = std::set<Entry>;

  LabelPropertyIndex(Indices *indices, Config::Items config, const VertexValidator &vertex_validator)
      : indices_(indices), config_(config), vertex_validator_{&vertex_validator} {}

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, VertexContainer &vertices);

  bool DropIndex(LabelId label, PropertyId property) { return index_.erase({label, property}) > 0; }

  bool IndexExists(LabelId label, PropertyId property) const { return index_.find({label, property}) != index_.end(); }

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const;

  void RemoveObsoleteEntries(uint64_t clean_up_before_timestamp);

  class Iterable {
   public:
    Iterable(IndexContainer &index_container, LabelId label, PropertyId property,
             const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Transaction *transaction,
             Indices *indices, Config::Items config, const VertexValidator &vertex_validator);

    class Iterator {
     public:
      Iterator(Iterable *self, IndexContainer::iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      IndexContainer::iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin();
    Iterator end();

   private:
    IndexContainer *index_container_;
    LabelId label_;
    PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Transaction *transaction_;
    Indices *indices_;
    Config::Items config_;
    const VertexValidator *vertex_validator_;
  };

  Iterable Vertices(LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                    Transaction *transaction) {
    auto it = index_.find({label, property});
    MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
              property.AsUint());
    return {it->second, label,       property, lower_bound, upper_bound,
            view,       transaction, indices_, config_,     *vertex_validator_};
  }

  int64_t VertexCount(LabelId label, PropertyId property) const {
    auto it = index_.find({label, property});
    MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
              property.AsUint());
    return static_cast<int64_t>(it->second.size());
  }

  /// Supplying a specific value into the count estimation function will return
  /// an estimated count of nodes which have their property's value set to
  /// `value`. If the `value` specified is `Null`, then an average number of
  /// equal elements is returned.
  int64_t VertexCount(LabelId label, PropertyId property, const PropertyValue &value) const;

  int64_t VertexCount(LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
                      const std::optional<utils::Bound<PropertyValue>> &upper) const;

  void Clear() { index_.clear(); }

  std::map<IndexType, IndexContainer> SplitIndexEntries(const PrimaryKey &split_key) {
    std::map<IndexType, IndexContainer> cloned_indices;
    for (auto &[index_type_val, index] : index_) {
      auto entry_it = index.begin();
      auto &cloned_index_container = cloned_indices[index_type_val];
      while (entry_it != index.end()) {
        // We need to save the next iterator since the current one will be
        // invalidated after extract
        auto next_entry_it = std::next(entry_it);
        if (entry_it->vertex->first > split_key) {
          [[maybe_unused]] const auto &[inserted_entry_it, inserted, node] =
              cloned_index_container.insert(index.extract(entry_it));
          MG_ASSERT(inserted, "Failed to extract index entry!");
        }
        entry_it = next_entry_it;
      }
    }

    return cloned_indices;
  }

 private:
  std::map<std::pair<LabelId, PropertyId>, IndexContainer> index_;
  Indices *indices_;
  Config::Items config_;
  const VertexValidator *vertex_validator_;
};

struct Indices {
  Indices(Config::Items config, const VertexValidator &vertex_validator)
      : label_index(this, config, vertex_validator), label_property_index(this, config, vertex_validator) {}

  // Disable copy and move because members hold pointer to `this`.
  Indices(const Indices &) = delete;
  Indices(Indices &&) = delete;
  Indices &operator=(const Indices &) = delete;
  Indices &operator=(Indices &&) = delete;
  ~Indices() = default;

  LabelIndex label_index;
  LabelPropertyIndex label_property_index;
};

/// This function should be called from garbage collection to clean-up the
/// index.
void RemoveObsoleteEntries(Indices *indices, uint64_t clean_up_before_timestamp);

// Indices are updated whenever an update occurs, instead of only on commit or
// advance command. This is necessary because we want indices to support `NEW`
// view for use in Merge.

/// This function should be called whenever a label is added to a vertex.
/// @throw std::bad_alloc
void UpdateOnAddLabel(Indices *indices, LabelId label, Vertex *vertex, const Transaction &tx);

/// This function should be called whenever a property is modified on a vertex.
/// @throw std::bad_alloc
void UpdateOnSetProperty(Indices *indices, PropertyId property, const PropertyValue &value, Vertex *vertex,
                         const Transaction &tx);
}  // namespace memgraph::storage::v3