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

#include "storage/custom_storage/storage.hpp"
#include "spdlog/spdlog.h"

namespace memgraph::storage::custom_storage {

void Storage::Call() { SPDLOG_WARN("Storage Call"); }

Vertex *Storage::AddVertex(Vertex &&vertex) {
  return vertices_.emplace_back(std::make_unique<Vertex>(std::move(vertex))).get();
}

uint64_t Storage::VerticesNo() const { return vertices_.size(); }

}  // namespace memgraph::storage::custom_storage
