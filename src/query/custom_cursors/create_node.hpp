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

#include "query/plan/cursor.hpp"

// TASKS:
//   1. check label creation access rule
//   2. create vertex
//   3. inform the trigger

namespace memgraph::query::plan {
class CreateNode;
}  // namespace memgraph::query::plan

namespace memgraph::query::custom_cursors {

class CreateNodeCursor : public memgraph::query::plan::Cursor {
 public:
  explicit CreateNodeCursor(const plan::CreateNode &logical_operator, plan::UniqueCursorPtr input_cursor);
  bool Pull(Frame &frame, ExecutionContext &context) override;
  void Shutdown() override;
  void Reset() override;

 private:
  const plan::CreateNode &logical_operator_;
  const plan::UniqueCursorPtr input_cursor_;
};

}  // namespace memgraph::query::custom_cursors