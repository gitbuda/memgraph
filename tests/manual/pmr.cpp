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

#include <algorithm>
#include <array>
#include <memory_resource>
#include <string>

#include <fmt/format.h>

// Credits to https://github.com/lefticus/cpp_weekly/blob/master/PMR/1_experiments.cpp
template <typename ItrBegin, typename ItrEnd>
void print_line(int offset, ItrBegin begin, const ItrEnd end) {
  fmt::print("(dec) {:02x}:  {:3}\n", offset, fmt::join(begin, end, "  "));
  fmt::print("(hex) {:02x}:   {:02x}\n", offset, fmt::join(begin, end, "   "));
  fmt::print("(asc) {:02x}:", offset);
  std::for_each(begin, end, [](const auto c) {
    if (std::isgraph(c)) {
      fmt::print("   {} ", static_cast<char>(c));
    } else {
      fmt::print(" \\{:03o}", c);
    }
  });
  fmt::print("\n");
}
template <typename Buffer, typename Container>
void print_buffer(const std::string_view title, const Buffer &buffer, const Container &container) {
  fmt::print("==============={:^10}==============\n", title);
  auto begin = buffer.begin();
  fmt::print("Buffer Address Start: {}\n", static_cast<const void *>(buffer.data()));
  fmt::print("Buffer Address End: {}\n", static_cast<const void *>(buffer.data() + buffer.size()));
  for (const auto &elem : container) {
    fmt::print(" Item Address: {}\n", static_cast<const void *>(&elem));
  }
  for (std::size_t offset = 0; offset < buffer.size(); offset += 16) {
    print_line(offset, std::next(begin, offset), std::next(begin, offset + 16));
  }
  fmt::print("\n");
}

// https://github.com/lefticus/cpp_weekly/blob/master/PMR/2_aa_type.cpp
// NOTE: For primitive fields allocator doesn't matter.
struct Node {
  int64_t id;
  std::pmr::string label;
  using allocator_type = std::pmr::polymorphic_allocator<>;

  explicit Node(const int64_t id, const std::string_view label, allocator_type alloc = {})
      : id(id), label(label, alloc) {}
  Node(const Node &other, allocator_type alloc = {}) : id(other.id), label(other.label, alloc) {}
  Node(Node &&) = default;
  Node(Node &&other, allocator_type alloc) : id(other.id), label(std::move(other.label), alloc) {}
  Node &operator=(const Node &rhs) = default;
  Node &operator=(Node &&rhs) = default;
  ~Node() = default;

  allocator_type get_allocator() const { return label.get_allocator(); }
};

int main() {
  std::array<std::uint8_t, 128> buffer1{};
  std::pmr::monotonic_buffer_resource pool1(buffer1.data(), buffer1.size());
  // NOTE: vector doesn't live in the buffer, only the data itself is inside the buffer.
  // NOTE: pmr objects are longer (std::string 32B, std::pmr::string 40B)
  // NOTE:
  //   * std::string => ptr_data + size + data + null
  //   * pmr::string => ptr_alloc + ptr_data + size + data + null
  std::pmr::vector<std::pmr::string> data1{&pool1};
  data1.reserve(2);
  print_buffer("initial", buffer1, "");
  data1.emplace_back("foo");
  print_buffer("data - foo", buffer1, data1);
  data1.emplace_back("a very long long bar string");
  print_buffer("data - foo & bar", buffer1, data1);

  std::array<std::uint8_t, 128> buffer2{};
  std::pmr::monotonic_buffer_resource pool2(buffer2.data(), buffer2.size());
  std::pmr::vector<Node> data2{&pool2};
  data2.reserve(2);
  print_buffer("initial", buffer2, "");
  data2.emplace_back(Node(77, "bla"));
  print_buffer("data", buffer2, data2);

  return 0;
}
