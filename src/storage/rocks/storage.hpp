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
#include <map>
#include <optional>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "utils/bound.hpp"
#include "utils/result.hpp"

namespace memgraph::storage::rocks {

static_assert(std::is_same_v<uint8_t, unsigned char>);
enum class Error : uint8_t {
  STORAGE_ERROR_XYZ,
};
template <class TValue>
using Result = utils::BasicResult<Error, TValue>;

struct Vertex {};
struct Edge {};

class AllVerticesIterable final {};
class VerticesIterable final {};

struct IndicesInfo {};
struct ConstraintsInfo {};
struct StorageInfo {};
struct StorageDataManipulationError {};

// TODO(gitbuda): Figure out how to position transactional stuff in the API
struct Transaction {};
struct IsolationLevel {};
struct View {};

struct Indices {};
struct Constraints {};
struct PropertyValue {};
struct Config {
  // properties_on_edges
};

struct EdgeRef {
  explicit EdgeRef(Gid gid) : gid(gid) {}
  explicit EdgeRef(Edge *ptr) : ptr(ptr) {}
  union {
    Gid gid;
    Edge *ptr;
  };
};
static_assert(sizeof(Gid) == sizeof(Edge *), "The Gid should be the same size as an Edge *!");
static_assert(std::is_standard_layout_v<Gid>, "The Gid must have a standard layout!");
static_assert(std::is_standard_layout_v<Edge *>, "The Edge * must have a standard layout!");
static_assert(std::is_standard_layout_v<EdgeRef>, "The EdgeRef must have a standard layout!");
inline bool operator==(const EdgeRef &a, const EdgeRef &b) noexcept { return a.gid == b.gid; }
inline bool operator!=(const EdgeRef &a, const EdgeRef &b) noexcept { return a.gid != b.gid; }

class EdgeAccessor;
class VertexAccessor final {
 private:
  friend class Storage;

 public:
  VertexAccessor(Vertex *vertex, Transaction *transaction, Indices *indices, Constraints *constraints, Config config,
                 bool for_deleted = false)
      : vertex_(vertex),
        transaction_(transaction),
        indices_(indices),
        constraints_(constraints),
        config_(config),
        for_deleted_(for_deleted) {}

  static std::optional<VertexAccessor> Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                              Constraints *constraints, Config config, View view) {
    throw 1;
  }
  bool IsVisible(View view) const { throw 1; }
  Result<bool> AddLabel(LabelId label) { throw 1; }
  Result<bool> RemoveLabel(LabelId label) { throw 1; }
  Result<bool> HasLabel(LabelId label, View view) const { throw 1; }
  Result<std::vector<LabelId>> Labels(View view) const { throw 1; }
  Result<PropertyValue> SetProperty(PropertyId property, const PropertyValue &value) { throw 1; }
  Result<std::map<PropertyId, PropertyValue>> ClearProperties() { throw 1; }
  Result<PropertyValue> GetProperty(PropertyId property, View view) const { throw 1; }
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const { throw 1; }
  Result<std::vector<EdgeAccessor>> InEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                            const VertexAccessor *destination = nullptr) const {
    throw 1;
  }
  Result<std::vector<EdgeAccessor>> OutEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                             const VertexAccessor *destination = nullptr) const {
    throw 1;
  }
  Result<size_t> InDegree(View view) const { throw 1; }
  Result<size_t> OutDegree(View view) const { throw 1; }
  Gid Gid() const noexcept { throw 1; }
  bool operator==(const VertexAccessor &other) const noexcept { throw 1; }
  bool operator!=(const VertexAccessor &other) const noexcept { throw 1; }

 private:
  Vertex *vertex_;
  Transaction *transaction_;
  Indices *indices_;
  Constraints *constraints_;
  Config config_;
  // if the accessor was created for a deleted vertex.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the node
  // even though the node is deleted.
  // All the write operations, and operators used for traversal (e.g. InEdges) will still
  // return an error if it's called for a deleted vertex.
  bool for_deleted_{false};
};

class EdgeAccessor final {
 private:
  friend class Storage;

 public:
  EdgeAccessor(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex, Transaction *transaction,
               Indices *indices, Constraints *constraints, Config config, bool for_deleted = false)
      : edge_(edge),
        edge_type_(edge_type),
        from_vertex_(from_vertex),
        to_vertex_(to_vertex),
        transaction_(transaction),
        indices_(indices),
        constraints_(constraints),
        config_(config),
        for_deleted_(for_deleted) {}

  bool IsVisible(View view) const { throw 1; }
  VertexAccessor FromVertex() const { throw 1; }
  VertexAccessor ToVertex() const { throw 1; }
  EdgeTypeId EdgeType() const { throw 1; }
  Result<PropertyValue> SetProperty(PropertyId property, const PropertyValue &value) { throw 1; }
  Result<std::map<PropertyId, PropertyValue>> ClearProperties() { throw 1; }
  Result<PropertyValue> GetProperty(PropertyId property, View view) const { throw 1; }
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const { throw 1; }
  Gid Gid() const noexcept { throw 1; }
  bool IsCycle() const { throw 1; }
  bool operator==(const EdgeAccessor &other) const noexcept {
    return edge_ == other.edge_ && transaction_ == other.transaction_;
  }
  bool operator!=(const EdgeAccessor &other) const noexcept { return !(*this == other); }

 private:
  EdgeRef edge_;
  EdgeTypeId edge_type_;
  Vertex *from_vertex_;
  Vertex *to_vertex_;
  Transaction *transaction_;
  Indices *indices_;
  Constraints *constraints_;
  Config config_;
  // if the accessor was created for a deleted edge.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the edge
  // even though the edge is deleted.
  // All the write operations will still return an error if it's called for a deleted edge.
  bool for_deleted_{false};
};

struct PathAccessor {};
struct SubgraphAccessor {};

class Storage final {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  /// TODO(gitbuda): Config as an argument.
  explicit Storage() = default;
  ~Storage() = default;

  class Accessor final {
   private:
    friend class Storage;
    explicit Accessor(Storage *storage, IsolationLevel isolation_level) { MG_ASSERT(false, "impl accessor ctor"); }

   public:
    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;
    Accessor &operator=(Accessor &&other) = delete;
    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    Accessor(Accessor &&other) noexcept { MG_ASSERT(false, "impl accessor move ctor"); }
    ~Accessor() {}

    /// @throw std::bad_alloc
    // TODO(gitbuda): Add data:
    //   * labels
    //   * primary key
    //   * other props
    VertexAccessor CreateVertex();
    std::optional<VertexAccessor> FindVertex(Gid git, View view) { throw 1; }
    Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex) { throw 1; }
    Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
        VertexAccessor *vertex) {
      throw 1;
    }
    // TODO(gitbuda): Raw vertices data, no accessor because of the ingestion speed.
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) { throw 1; }
    Result<std::optional<EdgeAccessor>> DeleteEdge(EdgeAccessor *edge) { throw 1; }

    VerticesIterable Vertices() { throw 1; }
    VerticesIterable Vertices(LabelId label, View view) { throw 1; }
    VerticesIterable Vertices(LabelId label, PropertyId property, View view) { throw 1; }
    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) { throw 1; }
    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) {
      throw 1;
    }

    int64_t ApproximateVertexCount() const { throw 1; }
    int64_t ApproximateVertexCount(LabelId label) const { throw 1; }
    int64_t ApproximateVertexCount(LabelId label, PropertyId property) const { throw 1; }
    int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const { throw 1; }
    int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                   const std::optional<utils::Bound<PropertyValue>> &lower,
                                   const std::optional<utils::Bound<PropertyValue>> &upper) const {
      throw 1;
    }

    const std::string &LabelToName(LabelId label) const { throw 1; }
    const std::string &PropertyToName(PropertyId property) const { throw 1; }
    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const { throw 1; }
    LabelId NameToLabel(std::string_view name) { throw 1; }
    PropertyId NameToProperty(std::string_view name) { throw 1; }
    EdgeTypeId NameToEdgeType(std::string_view name) { throw 1; }

    IndicesInfo ListAllIndices() const { throw 1; }
    bool LabelIndexExists(LabelId label) const { throw 1; }
    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const { throw 1; }
    ConstraintsInfo ListAllConstraints() const { throw 1; }

    void AdvanceCommand() { throw 1; }
    utils::BasicResult<StorageDataManipulationError, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp = {}) {
      throw 1;
    }
    void Abort() { throw 1; }
    void FinalizeTransaction() { throw 1; }

   private:
    // Storage *storage_;
    // std::shared_lock<utils::RWLock> storage_guard_;
    // Transaction transaction_;
    // std::optional<uint64_t> commit_timestamp_;
    // bool is_transaction_active_;
    // Config::Items config_;
  };

  Accessor Access(std::optional<IsolationLevel> override_isolation_level = {}) { throw 1; }
};

}  // namespace memgraph::storage::rocks
