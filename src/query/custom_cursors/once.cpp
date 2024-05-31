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

#include "query/custom_cursors/once.hpp"
#include "query/context.hpp"
#include "query/custom_cursors/utils.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/scoped_profile.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::custom_cursors {

bool OnceCursor::Pull(Frame & /*unused*/, ExecutionContext &context) {
  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  memgraph::query::plan::ScopedProfile profile{ComputeProfilingKey(this), "Once", &context};
  SPDLOG_WARN("OnceCursor::Pull");
  if (!did_pull_) {
    did_pull_ = true;
    return true;
  }
  return false;
}

void OnceCursor::Shutdown() {}

void OnceCursor::Reset() { did_pull_ = false; }

}  // namespace memgraph::query::custom_cursors
