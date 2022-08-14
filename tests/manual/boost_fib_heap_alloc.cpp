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

#include <iostream>

#include <boost/heap/fibonacci_heap.hpp>
#include <utils/memory.hpp>
#include <utils/pmr/vector.hpp>

int main(int, const char **) {
  boost::heap::fibonacci_heap<int, boost::heap::allocator<std::allocator<int>>> heap;
  heap.push(1);
  heap.push(2);
  std::cout << heap.size() << std::endl;

  auto mem = memgraph::utils::NewDeleteResource();
  // This seems to be right because underlying vector is using the mem MemoryResource.
  std::priority_queue<int, memgraph::utils::pmr::vector<int>()> pq{mem};
  pq.push(1);
  pq.push(2);
  std::cout << pq.size() << std::endl;

  return 0;
}
