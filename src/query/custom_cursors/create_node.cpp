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

#include "query/custom_cursors/create_node.hpp"
#include "query/context.hpp"
#include "query/custom_cursors/utils.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/scoped_profile.hpp"
#include "storage/custom_storage/types.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::custom_cursors {

struct QueryVertex {};

// Creates a vertex on this GraphDb. Returns a reference to vertex placed on the
// frame.
QueryVertex CreateVertex(const plan::NodeCreationInfo &node_info, Frame *frame, ExecutionContext &context) {
  auto &dba = *context.db_accessor;
  if (node_info.labels.size() != 1) {
    throw QueryRuntimeException(
        "0 or multiple labels not yet supported under CreateNode. You have to provide exactly 1 lable for any given "
        "vertex/node.");
  }
  // NOTE: Evaluator should use the latest accessors, as modified in this query, when
  // setting properties on new nodes.
  // NOTE: Evaluator is using query::DBAccessor of default storage mode (IN_MEM_TX), for props mapping & storage mode.
  ExpressionEvaluator evaluator(frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                storage::View::NEW);
  // TODO: PropsSetChecked allocates a PropertyValue, make it use context.memory
  // when we update PropertyValue with custom allocator.
  std::map<storage::PropertyId, storage::PropertyValue> properties;
  if (const auto *node_info_properties = std::get_if<plan::PropertiesMapList>(&node_info.properties)) {
    for (const auto &[key, value_expression] : *node_info_properties) {
      properties.emplace(key, value_expression->Accept(evaluator));
    }
  } else {
    auto property_map = evaluator.Visit(*std::get<ParameterLookup *>(node_info.properties));
    for (const auto &[key, value] : property_map.ValueMap()) {
      properties.emplace(dba.NameToProperty(key), value);
    }
  }
  // TODO(gitbuda): Put vertex on the frame. (*frame)[node_info.symbol] = new_node;
  // (*frame)[node_info.symbol] = new_node;
  // return (*frame)[node_info.symbol].ValueVertex();

  // TODO(gitbuda): node_info.labels change type -> add the transformation.
  // auto new_node = memgraph::storage::custom_storage::Vertex{.labels = node_info.labels, .properties = properties};
  auto new_node = memgraph::storage::custom_storage::Vertex{.labels = {}, .properties = properties};
  auto *vertex_ptr = context.custom_storage->AddVertex(std::move(new_node));
  SPDLOG_WARN("{}", context.custom_storage->VerticesNo());
  return QueryVertex{};
}

CreateNodeCursor::CreateNodeCursor(const plan::CreateNode &logical_operator, plan::UniqueCursorPtr input_cursor)
    : logical_operator_(logical_operator), input_cursor_(std::move(input_cursor)) {}

bool CreateNodeCursor::Pull(Frame &frame, ExecutionContext &context) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  memgraph::query::plan::ScopedProfile profile{ComputeProfilingKey(this), "CreateNode", &context};
  SPDLOG_WARN("CreateNodeCursor::Pull");
  if (input_cursor_->Pull(frame, context)) {
    CreateVertex(logical_operator_.node_info_, &frame, context);
    return true;
  }
  return false;
}

void CreateNodeCursor::Shutdown() { input_cursor_->Shutdown(); }

void CreateNodeCursor::Reset() { input_cursor_->Reset(); }

}  // namespace memgraph::query::custom_cursors
