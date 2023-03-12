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

#include "slk/serialization.hpp"
#include "storage/rocks/vertex.hpp"

namespace memgraph::slk {

void Save(const storage::rocks::Vertex &value, slk::Builder *builder) { slk::Save(value.id, builder); }
void Load(storage::rocks::Vertex *value, slk::Reader *reader) {
  int64_t id;
  slk::Load(&id, reader);
  value->id = id;
}

}  // namespace memgraph::slk
