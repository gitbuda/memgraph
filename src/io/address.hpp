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

#include <compare>

#include <fmt/format.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "utils/logging.hpp"

namespace memgraph::io {

struct PartialAddress {
  boost::asio::ip::address ip;
  uint16_t port;

  friend bool operator==(const PartialAddress &lhs, const PartialAddress &rhs) = default;

  /// unique_id is most dominant for ordering, then ip, then port
  friend bool operator<(const PartialAddress &lhs, const PartialAddress &rhs) {
    if (lhs.ip != rhs.ip) {
      return lhs.ip < rhs.ip;
    }

    return lhs.port < rhs.port;
  }

  std::string ToString() const { return fmt::format("PartialAddress {{ ip: {}, port: {} }}", ip.to_string(), port); }

  friend std::ostream &operator<<(std::ostream &in, const PartialAddress &partial_address) {
    in << partial_address.ToString();
    return in;
  }
};

struct Address {
  // It's important for all participants to have a
  // unique identifier - IP and port alone are not
  // enough, and may change over the lifecycle of
  // the nodes. Particularly storage nodes may change
  // their IP addresses over time, and the system
  // should gracefully update its information
  // about them.
  boost::uuids::uuid unique_id;
  boost::asio::ip::address last_known_ip;
  uint16_t last_known_port;

  static Address TestAddress(uint16_t port) {
    MG_ASSERT(port <= 255);

    return Address{
        .unique_id = boost::uuids::uuid{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, static_cast<unsigned char>(port)},
        .last_known_port = port,
    };
  }

  // NB: don't use this in test code because it is non-deterministic
  static Address UniqueLocalAddress() {
    return Address{
        .unique_id = boost::uuids::uuid{boost::uuids::random_generator()()},
    };
  }

  /// `Coordinator`s have constant UUIDs because there is at most one per ip/port pair.
  Address ForkLocalCoordinator() {
    return Address{
        .unique_id = boost::uuids::uuid{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        .last_known_ip = last_known_ip,
        .last_known_port = last_known_port,
    };
  }

  /// `ShardManager`s have constant UUIDs because there is at most one per ip/port pair.
  Address ForkLocalShardManager() {
    return Address{
        .unique_id = boost::uuids::uuid{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        .last_known_ip = last_known_ip,
        .last_known_port = last_known_port,
    };
  }

  /// Returns a new ID with the same IP and port but a unique UUID.
  Address ForkUniqueAddress() {
    return Address{
        .unique_id = boost::uuids::uuid{boost::uuids::random_generator()()},
        .last_known_ip = last_known_ip,
        .last_known_port = last_known_port,
    };
  }

  PartialAddress ToPartialAddress() const {
    return PartialAddress{
        .ip = last_known_ip,
        .port = last_known_port,
    };
  }

  friend bool operator==(const Address &lhs, const Address &rhs) = default;

  /// unique_id is most dominant for ordering, then last_known_ip, then last_known_port
  friend bool operator<(const Address &lhs, const Address &rhs) {
    if (lhs.unique_id != rhs.unique_id) {
      return lhs.unique_id < rhs.unique_id;
    }

    if (lhs.last_known_ip != rhs.last_known_ip) {
      return lhs.last_known_ip < rhs.last_known_ip;
    }

    return lhs.last_known_port < rhs.last_known_port;
  }

  std::string ToString() const {
    return fmt::format("Address {{ unique_id: {}, last_known_ip: {}, last_known_port: {} }}",
                       boost::uuids::to_string(unique_id), last_known_ip.to_string(), last_known_port);
  }

  friend std::ostream &operator<<(std::ostream &in, const Address &address) {
    in << address.ToString();
    return in;
  }
};

};  // namespace memgraph::io

namespace std {
template <>
struct hash<memgraph::io::PartialAddress> {
  size_t operator()(const memgraph::io::PartialAddress &pa) const {
    using boost::hash_combine;
    using boost::hash_value;

    // Start with a hash value of 0    .
    std::size_t seed = 0;

    if (pa.ip.is_v4()) {
      auto h = std::hash<boost::asio::ip::address_v4>()(pa.ip.to_v4());
      hash_combine(seed, h);
    } else {
      auto h = std::hash<boost::asio::ip::address_v6>()(pa.ip.to_v6());
      hash_combine(seed, h);
    }
    hash_combine(seed, hash_value(pa.port));

    // Return the result.
    return seed;
  }
};
}  // namespace std