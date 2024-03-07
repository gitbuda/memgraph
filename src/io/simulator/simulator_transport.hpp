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

#include <memory>
#include <utility>

#include "io/address.hpp"
#include "io/notifier.hpp"
#include "io/simulator/simulator_handle.hpp"
#include "io/time.hpp"

namespace memgraph::io::simulator {

using memgraph::io::Duration;
using memgraph::io::Time;

class SimulatorTransport {
  std::shared_ptr<SimulatorHandle> simulator_handle_;
  Address address_;
  std::mt19937 rng_;

 public:
  SimulatorTransport(std::shared_ptr<SimulatorHandle> simulator_handle, Address address, uint64_t seed)
      : simulator_handle_(simulator_handle), address_(address), rng_(std::mt19937{seed}) {}

  template <Message ResponseT, Message RequestT>
  ResponseFuture<ResponseT> Request(Address to_address, Address from_address, RValueRef<RequestT> request,
                                    std::function<void()> notification, Duration timeout) {
    std::function<bool()> tick_simulator = [handle_copy = simulator_handle_] {
      return handle_copy->MaybeTickSimulator();
    };

    return simulator_handle_->template SubmitRequest<ResponseT, RequestT>(
        to_address, from_address, std::move(request), timeout, std::move(tick_simulator), std::move(notification));
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Address receiver_address, Duration timeout) {
    return simulator_handle_->template Receive<Ms...>(receiver_address, timeout);
  }

  template <Message M>
  void Send(Address to_address, Address from_address, uint64_t request_id, RValueRef<M> message) {
    return simulator_handle_->template Send<M>(to_address, from_address, request_id, std::move(message));
  }

  Time Now() const { return simulator_handle_->Now(); }

  bool ShouldShutDown() const { return simulator_handle_->ShouldShutDown(); }

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    return distrib(rng_);
  }

  LatencyHistogramSummaries ResponseLatencies() { return simulator_handle_->ResponseLatencies(); }
};
};  // namespace memgraph::io::simulator