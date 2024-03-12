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

namespace memgraph::storage::custom_storage {

// Desing ideas:
//   * try to follow existing API design
//     * one of the issue is granular and SYNC API design
//   * try to reuse exisitng in-memory data structures
//     * PropertyStore + all PropertyValues seem very reusable
//     * Vertex/Edge seems not very reusable because (delta*, edges*)
//   * parallelization
//   * ASYNC disk/network access
//   * per database, maybe even isolated data cache

// Target queries:
//   CREATE (:Label {props});                                                            // single vertex create
//   UNWIND vertices_props AS props CREATE (n:Label) SET n += props;                     // batch  vertex create
//   MATCH (n:Label {id:X}) RETURN n;                                                    // single vertex lookup
//   MATCH (n1:Label {id:X}) MATCH (n2:Label {id:Y}) CREATE (n1)-[r:Type {props}]->(n2); // single edge   create
//   // batch edge create
//   // iterate all verteices with limited memory usage -> "global" graph algos possible
//   // iterate all edges with limited memory usage     -> "global" graph algos possible
//   // get IN/OUT/ALL edges for a given vertex         -> "global" graph algos possible
//   // BFS with filter lambda

class Storage {
 public:
  void Call();
};

}  // namespace memgraph::storage::custom_storage
