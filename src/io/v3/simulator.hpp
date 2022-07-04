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

#include <variant>

#include "address.hpp"
#include "errors.hpp"
#include "future.hpp"
#include "simulator_handle.hpp"
#include "transport.hpp"

struct SimulatorStats {
  uint64_t total_messages_;
  uint64_t dropped_messages_;
  uint64_t total_requests_;
  uint64_t total_responses_;
  uint64_t simulator_ticks_;
};

struct SimulatorConfig {
  uint8_t drop_percent_;
  uint64_t rng_seed_;
};

class Simulator {
 public:
  SimulatorTransport Register(Address address, bool is_server) {
    return SimulatorTransport(simulator_handle_, address);
  }

 private:
  std::shared_ptr<SimulatorHandle> simulator_handle_;
};
