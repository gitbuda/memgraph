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

#include <cstdint>

#include "query/plan/scoped_profile.hpp"

template <typename T>
uint64_t ComputeProfilingKey(const T *obj) {
  static_assert(sizeof(T *) == sizeof(uint64_t));
  return reinterpret_cast<uint64_t>(obj);
}
