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

#include <chrono>
#include <memory>
#include <random>
#include <utility>

#include "io/address.hpp"
#include "io/local_transport/local_transport_handle.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::io::local_transport {

class LocalTransport {
  std::shared_ptr<LocalTransportHandle> local_transport_handle_;

 public:
  explicit LocalTransport(std::shared_ptr<LocalTransportHandle> local_transport_handle)
      : local_transport_handle_(std::move(local_transport_handle)) {}

  template <Message ResponseT, Message RequestT>
  ResponseFuture<ResponseT> Request(Address to_address, Address from_address, RValueRef<RequestT> request,
                                    std::function<void()> fill_notifier, Duration timeout) {
    return local_transport_handle_->template SubmitRequest<ResponseT, RequestT>(
        to_address, from_address, std::move(request), timeout, fill_notifier);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Address receiver_address, Duration timeout) {
    return local_transport_handle_->template Receive<Ms...>(receiver_address, timeout);
  }

  template <Message M>
  void Send(Address to_address, Address from_address, RequestId request_id, RValueRef<M> message) {
    return local_transport_handle_->template Send<M>(to_address, from_address, request_id, std::move(message));
  }

  Time Now() const { return local_transport_handle_->Now(); }

  bool ShouldShutDown() const { return local_transport_handle_->ShouldShutDown(); }

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    std::random_device rng;
    return distrib(rng);
  }

  LatencyHistogramSummaries ResponseLatencies() { return local_transport_handle_->ResponseLatencies(); }
};
};  // namespace memgraph::io::local_transport