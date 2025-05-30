// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/db_accessor.hpp"

#include "query/graph.hpp"

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>
#include "storage/v2/storage_mode.hpp"
#include "utils/pmr/unordered_set.hpp"

namespace memgraph::query {
SubgraphDbAccessor::SubgraphDbAccessor(query::DbAccessor db_accessor, Graph *graph)
    : db_accessor_(db_accessor), graph_(graph) {}

void SubgraphDbAccessor::TrackCurrentThreadAllocations() { return db_accessor_.TrackCurrentThreadAllocations(); }

void SubgraphDbAccessor::UntrackCurrentThreadAllocations() { return db_accessor_.UntrackCurrentThreadAllocations(); }

storage::PropertyId SubgraphDbAccessor::NameToProperty(const std::string_view name) {
  return db_accessor_.NameToProperty(name);
}

storage::LabelId SubgraphDbAccessor::NameToLabel(const std::string_view name) { return db_accessor_.NameToLabel(name); }

storage::EdgeTypeId SubgraphDbAccessor::NameToEdgeType(const std::string_view name) {
  return db_accessor_.NameToEdgeType(name);
}

const std::string &SubgraphDbAccessor::PropertyToName(storage::PropertyId prop) const {
  return db_accessor_.PropertyToName(prop);
}

const std::string &SubgraphDbAccessor::LabelToName(storage::LabelId label) const {
  return db_accessor_.LabelToName(label);
}

const std::string &SubgraphDbAccessor::EdgeTypeToName(storage::EdgeTypeId type) const {
  return db_accessor_.EdgeTypeToName(type);
}

storage::Result<std::optional<EdgeAccessor>> SubgraphDbAccessor::RemoveEdge(EdgeAccessor *edge) {
  if (!this->graph_->ContainsEdge(*edge)) {
    throw std::logic_error{"Projected graph must contain edge!"};
  }
  auto result = db_accessor_.RemoveEdge(edge);
  if (result.HasError() || !*result) {
    return result;
  }
  return this->graph_->RemoveEdge(*edge);
}

storage::Result<EdgeAccessor> SubgraphDbAccessor::InsertEdge(SubgraphVertexAccessor *from, SubgraphVertexAccessor *to,
                                                             const storage::EdgeTypeId &edge_type) {
  VertexAccessor *from_impl = &from->impl_;
  VertexAccessor *to_impl = &to->impl_;
  if (!this->graph_->ContainsVertex(*from_impl) || !this->graph_->ContainsVertex(*to_impl)) {
    throw std::logic_error{"Projected graph must contain both vertices to insert edge!"};
  }
  auto result = db_accessor_.InsertEdge(from_impl, to_impl, edge_type);
  if (result.HasError()) {
    return result;
  }
  this->graph_->InsertEdge(*result);
  return result;
}

storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>
SubgraphDbAccessor::DetachRemoveVertex(  // NOLINT(readability-convert-member-functions-to-static)
    SubgraphVertexAccessor *) {          // NOLINT(hicpp-named-parameter)
  throw std::logic_error{
      "Vertex holds only partial information about edges. Cannot detach delete safely while using projected graph."};
}

storage::Result<std::optional<VertexAccessor>> SubgraphDbAccessor::RemoveVertex(
    SubgraphVertexAccessor *subgraphvertex_accessor) {
  VertexAccessor *vertex_accessor = &subgraphvertex_accessor->impl_;
  if (!this->graph_->ContainsVertex(*vertex_accessor)) {
    throw std::logic_error{"Projected graph must contain vertex!"};
  }
  auto result = db_accessor_.RemoveVertex(vertex_accessor);
  if (result.HasError() || !*result) {
    return result;
  }
  return this->graph_->RemoveVertex(*vertex_accessor);
}

SubgraphVertexAccessor SubgraphDbAccessor::InsertVertex() {
  VertexAccessor vertex = db_accessor_.InsertVertex();
  this->graph_->InsertVertex(vertex);
  return SubgraphVertexAccessor(vertex, this->getGraph());
}

VerticesIterable SubgraphDbAccessor::Vertices(storage::View) {  // NOLINT(hicpp-named-parameter)
  return VerticesIterable(&graph_->vertices());
}

std::optional<VertexAccessor> SubgraphDbAccessor::FindVertex(storage::Gid gid, storage::View view) {
  std::optional<VertexAccessor> maybe_vertex = db_accessor_.FindVertex(gid, view);
  if (maybe_vertex && this->graph_->ContainsVertex(*maybe_vertex)) {
    return *maybe_vertex;
  }
  return std::nullopt;
}

query::Graph *SubgraphDbAccessor::getGraph() { return graph_; }

storage::StorageMode SubgraphDbAccessor::GetStorageMode() const noexcept { return db_accessor_.GetStorageMode(); }

DbAccessor *SubgraphDbAccessor::GetAccessor() { return &db_accessor_; }

VertexAccessor SubgraphVertexAccessor::GetVertexAccessor() const { return impl_; }

storage::Result<EdgeVertexAccessorResult> SubgraphVertexAccessor::OutEdges(storage::View view) const {
  auto maybe_edges = impl_.impl_.OutEdges(view, {});
  if (maybe_edges.HasError()) return maybe_edges.GetError();
  auto edges = std::move(maybe_edges->edges);
  const auto &graph_edges = graph_->edges();

  std::vector<storage::EdgeAccessor> filteredOutEdges;
  for (auto &edge : edges) {
    auto edge_q = EdgeAccessor(edge);
    if (graph_edges.contains(edge_q)) {
      filteredOutEdges.push_back(edge);
    }
  }

  std::vector<EdgeAccessor> resulting_edges;
  resulting_edges.reserve(filteredOutEdges.size());
  std::ranges::transform(filteredOutEdges, std::back_inserter(resulting_edges),
                         [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = std::move(resulting_edges), .expanded_count = maybe_edges->expanded_count};
}

storage::Result<EdgeVertexAccessorResult> SubgraphVertexAccessor::InEdges(storage::View view) const {
  auto maybe_edges = impl_.impl_.InEdges(view, {});
  if (maybe_edges.HasError()) return maybe_edges.GetError();
  auto edges = std::move(maybe_edges->edges);
  const auto &graph_edges = graph_->edges();

  std::vector<storage::EdgeAccessor> filteredOutEdges;
  for (auto &edge : edges) {
    auto edge_q = EdgeAccessor(edge);
    if (graph_edges.contains(edge_q)) {
      filteredOutEdges.push_back(edge);
    }
  }

  std::vector<EdgeAccessor> resulting_edges;
  resulting_edges.reserve(filteredOutEdges.size());
  std::ranges::transform(filteredOutEdges, std::back_inserter(resulting_edges),
                         [](auto const &edge) { return EdgeAccessor(edge); });

  return EdgeVertexAccessorResult{.edges = std::move(resulting_edges), .expanded_count = maybe_edges->expanded_count};
}

auto DbAccessor::PointVertices(storage::LabelId label, storage::PropertyId property,
                               storage::CoordinateReferenceSystem crs, TypedValue const &point_value,
                               TypedValue const &boundary_value, plan::PointDistanceCondition condition)
    -> PointIterable {
  return PointIterable(
      accessor_->PointVertices(label, property, crs, point_value.ToPropertyValue(accessor_->GetNameIdMapper()),
                               boundary_value.ToPropertyValue(accessor_->GetNameIdMapper()), condition));
}

auto DbAccessor::PointVertices(storage::LabelId label, storage::PropertyId property,
                               storage::CoordinateReferenceSystem crs, TypedValue const &bottom_left,
                               TypedValue const &top_right, plan::WithinBBoxCondition condition) -> PointIterable {
  return PointIterable(accessor_->PointVertices(label, property, crs,
                                                bottom_left.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                top_right.ToPropertyValue(accessor_->GetNameIdMapper()), condition));
}
}  // namespace memgraph::query
