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

#include <map>
#include <memory>
#include <optional>
#include <set>

#include "storage/v3/config.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/edge.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"

namespace memgraph::storage::v3 {

// If edge properties-on-edges is false then we don't need to send edges but
// only vertices, since they will contain those edges
struct SplitData {
  VertexContainer vertices;
  std::optional<EdgeContainer> edges;
  std::map<uint64_t, Transaction> transactions;
  std::map<LabelId, LabelIndex::LabelIndexContainer> label_indices;
  std::map<std::pair<LabelId, PropertyId>, LabelPropertyIndex::LabelPropertyIndexContainer> label_property_indices;
};

class Splitter final {
 public:
  Splitter(VertexContainer &vertices, EdgeContainer &edges,
           std::map<uint64_t, std::unique_ptr<Transaction>> &start_logical_id_to_transaction, Indices &indices,
           Config &config);

  Splitter(const Splitter &) = delete;
  Splitter(Splitter &&) noexcept = delete;
  Splitter &operator=(const Splitter &) = delete;
  Splitter operator=(Splitter &&) noexcept = delete;
  ~Splitter() = default;

  SplitData SplitShard(const PrimaryKey &split_key);

 private:
  std::map<uint64_t, Transaction> CollectTransactions(const std::set<uint64_t> &collected_transactions_start_id,
                                                      VertexContainer &cloned_vertices, EdgeContainer &cloned_edges);

  VertexContainer CollectVertices(SplitData &data, std::set<uint64_t> &collected_transactions_start_id,
                                  const PrimaryKey &split_key);

  std::optional<EdgeContainer> CollectEdges(std::set<uint64_t> &collected_transactions_start_id,
                                            const VertexContainer &split_vertices, const PrimaryKey &split_key);

  std::map<LabelId, LabelIndex::LabelIndexContainer> CollectLabelIndices(
      const PrimaryKey &split_key,
      std::map<LabelId, std::multimap<const Vertex *, LabelIndex::Entry *>> &vertex_entry_map);

  std::map<std::pair<LabelId, PropertyId>, LabelPropertyIndex::LabelPropertyIndexContainer> CollectLabelPropertyIndices(
      const PrimaryKey &split_key,
      std::map<std::pair<LabelId, PropertyId>, std::multimap<const Vertex *, LabelPropertyIndex::Entry *>>
          &vertex_entry_map);

  static void ScanDeltas(std::set<uint64_t> &collected_transactions_start_id, Delta *delta);

  static void AlignClonedTransaction(Transaction &cloned_transaction, const Transaction &transaction,
                                     std::map<uint64_t, Transaction> &cloned_transactions,
                                     VertexContainer &cloned_vertices, EdgeContainer &cloned_edges);

  void AlignClonedTransactions(std::map<uint64_t, Transaction> &cloned_transactions, VertexContainer &cloned_vertices,
                               EdgeContainer &cloned_edges);

  VertexContainer &vertices_;
  EdgeContainer &edges_;
  std::map<uint64_t, std::unique_ptr<Transaction>> &start_logical_id_to_transaction_;
  Indices &indices_;
  Config &config_;
};

}  // namespace memgraph::storage::v3
