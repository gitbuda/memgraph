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
#include <vector>

#include "utils/bound.hpp"
#include "utils/result.hpp"

namespace memgraph::storage::rocks {

static_assert(std::is_same_v<uint8_t, unsigned char>);
enum class Error : uint8_t {
  STORAGE_ERROR_XYZ,
};
template <class TValue>
using Result = utils::BasicResult<Error, TValue>;

class AllVerticesIterable final {};
class VerticesIterable final {};

struct IndicesInfo {};
struct ConstraintsInfo {};
struct StorageInfo {};
struct StorageDataManipulationError {};

struct Config {};
struct EdgeTypeId {};
struct Gid {};
struct IsolationLevel {};
struct LabelId {};
struct PropertyId {};
struct PropertyValue {};
struct View {};

// TODO(gitbuda): Implement granular accessor objects (a lot).
struct VertexAccessor {};
struct EdgeAccessor {};
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
    VertexAccessor CreateVertex() { throw 1; }
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
