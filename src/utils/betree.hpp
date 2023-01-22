// Copyright 2022 Memgraph Ltd.
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

#include <mutex>

namespace memgraph::utils {

// TODO(gitbuda): Once the API is in place, define the concept.
template <typename TStore, typename TKey, typename TValue>
concept KVStore = requires(TStore s, TKey k, TValue v) {
  { s.Get(k) } -> std::same_as<std::optional<TValue>>;
  { s.Delete(k) } -> std::same_as<bool>;
  { s.Put(k, v) } -> std::same_as<std::optional<TValue>>;
  { s.Size() } -> std::same_as<uint64_t>;
  // TODO(gitbuda): Add RangeGet(k1, k2) -> array.
};

enum class MessageType {
  kInsert = 0,
  kDelete,
  kUpdate,
};

template <typename TKey, typename TTime = uint64_t>
struct MesssageKey {
  TKey key;
  TTime time;
};

template <typename TValue>
struct MessageValue {
  MessageType type;
  TValue value;
};

template <typename TKey, typename TValue>
class BeTree {
  // NodeMaxSize
  // NodeMinFlushSize
};

/// Baseline implementation to define the API and benchmark.
template <typename TKey, typename TValue>
class ConcurrentUnorderedMap {
 public:
  std::optional<TValue> Get(TKey key) const {
    std::unique_lock guard{mutex_};
    auto search = data_.find(key);
    if (search != data_.end()) {
      return search.second();
    }
    return std::nullopt;
  }

  std::optional<TValue> Put(TKey key, TValue value) {
    std::unique_lock guard{mutex_};
    auto result = data_.insert_or_assign(key, value);
    return (*result.first).second;
  }

  auto Size() const {
    std::unique_lock guard{mutex_};
    return data_.size();
  }

 private:
  std::unordered_map<TKey, TValue> data_;
  std::mutex mutex_;
};

}  // namespace memgraph::utils
