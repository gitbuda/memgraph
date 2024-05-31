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

#include "query/custom_cursors/scanall.hpp"
#include "query/context.hpp"
#include "query/custom_cursors/utils.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/scoped_profile.hpp"

namespace memgraph::query::custom_cursors {

ScanAllCursor::ScanAllCursor(Symbol output_symbol, plan::UniqueCursorPtr input_cursor)
    : output_symbol_(std::move(output_symbol)), input_cursor_(std::move(input_cursor)) {}

bool ScanAllCursor::Pull(Frame &frame, ExecutionContext &context) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  memgraph::query::plan::ScopedProfile profile{ComputeProfilingKey(this), "ScanAll", &context};
  SPDLOG_WARN("ScanAll");
  context.custom_storage->Call();
  return input_cursor_->Pull(frame, context);
}

void ScanAllCursor::Shutdown() { input_cursor_->Shutdown(); }

void ScanAllCursor::Reset() { input_cursor_->Reset(); }

}  // namespace memgraph::query::custom_cursors
