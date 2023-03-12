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

#include <optional>

#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "storage/rocks/vertex.hpp"

namespace memgraph::slk {

void Save(const storage::rocks::Vertex &value, slk::Builder *builder) { slk::Save(value.id, builder); }
void Load(storage::rocks::Vertex *value, slk::Reader *reader) {
  int64_t id;
  slk::Load(&id, reader);
  value->id = id;
}

}  // namespace memgraph::slk

namespace memgraph::storage::rocks {

class Encoder final {
 public:
  explicit Encoder(slk::Builder *builder) : builder_(builder) {}
  void WriteVertex(const Vertex &value) { slk::Save(value, builder_); }

 private:
  slk::Builder *builder_;
};

class Decoder final {
 public:
  explicit Decoder(slk::Reader *reader) : reader_(reader) {}
  std::optional<Vertex> ReadVertex() {
    Vertex vertex;
    slk::Load(&vertex, reader_);
    return vertex;
  }

 private:
  slk::Reader *reader_;
};

}  // namespace memgraph::storage::rocks
