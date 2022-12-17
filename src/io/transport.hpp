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

#include <chrono>
#include <concepts>
#include <random>
#include <variant>

#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/future.hpp"
#include "io/message_histogram_collector.hpp"
#include "io/notifier.hpp"
#include "io/time.hpp"
#include "utils/result.hpp"

namespace memgraph::io {

using memgraph::utils::BasicResult;

// TODO(tyler) ensure that Message continues to represent
// reasonable constraints around message types over time,
// as we adapt things to use Thrift-generated message types.
template <typename T>
concept Message = std::same_as<T, std::decay_t<T>>;

using RequestId = uint64_t;

template <Message M>
struct ResponseEnvelope {
  M message;
  RequestId request_id;
  Address to_address;
  Address from_address;
  Duration response_latency;
};

template <Message M>
using ResponseResult = BasicResult<TimedOut, ResponseEnvelope<M>>;

template <Message M>
using ResponseFuture = memgraph::io::Future<ResponseResult<M>>;

template <Message M>
using ResponsePromise = memgraph::io::Promise<ResponseResult<M>>;

template <Message... Ms>
struct RequestEnvelope {
  std::variant<Ms...> message;
  RequestId request_id;
  Address to_address;
  Address from_address;
};

template <Message... Ms>
using RequestResult = BasicResult<TimedOut, RequestEnvelope<Ms...>>;

template <typename I>
class Io {
  I implementation_;
  Address address_;
  Duration default_timeout_ = std::chrono::microseconds{100000};

 public:
  Io(I io, Address address) : implementation_(io), address_(address) {}

  /// Set the default timeout for all requests that are issued
  /// without an explicit timeout set.
  void SetDefaultTimeout(Duration timeout) { default_timeout_ = timeout; }

  /// Returns the current default timeout for this Io instance.
  Duration GetDefaultTimeout() { return default_timeout_; }

  /// Issue a request with an explicit timeout in microseconds provided. This tends to be used by clients.
  template <Message RequestT, Message ResponseT>
  ResponseFuture<ResponseT> RequestWithTimeout(Address address, RequestT request, Duration timeout) {
    const Address from_address = address_;
    std::function<void()> fill_notifier = nullptr;
    return implementation_.template Request<RequestT, ResponseT>(address, from_address, request, fill_notifier,
                                                                 timeout);
  }

  /// Issue a request that times out after the default timeout. This tends
  /// to be used by clients.
  template <Message RequestT, Message ResponseT>
  ResponseFuture<ResponseT> Request(Address to_address, RequestT request) {
    const Duration timeout = default_timeout_;
    const Address from_address = address_;
    std::function<void()> fill_notifier = nullptr;
    return implementation_.template Request<RequestT, ResponseT>(to_address, from_address, std::move(request),
                                                                 fill_notifier, timeout);
  }

  /// Issue a request that will notify a Notifier when it is filled or times out.
  template <Message RequestT, Message ResponseT>
  ResponseFuture<ResponseT> RequestWithNotification(Address to_address, RequestT request, Notifier notifier,
                                                    ReadinessToken readiness_token) {
    const Duration timeout = default_timeout_;
    const Address from_address = address_;
    std::function<void()> fill_notifier = [notifier, readiness_token]() { notifier.Notify(readiness_token); };
    return implementation_.template Request<RequestT, ResponseT>(to_address, from_address, std::move(request),
                                                                 fill_notifier, timeout);
  }

  /// Issue a request that will notify a Notifier when it is filled or times out.
  template <Message RequestT, Message ResponseT>
  ResponseFuture<ResponseT> RequestWithNotificationAndTimeout(Address to_address, RequestT request, Notifier notifier,
                                                              ReadinessToken readiness_token, Duration timeout) {
    const Address from_address = address_;
    std::function<void()> fill_notifier = [notifier, readiness_token]() { notifier.Notify(readiness_token); };
    return implementation_.template Request<RequestT, ResponseT>(to_address, from_address, std::move(request),
                                                                 fill_notifier, timeout);
  }

  /// Wait for an explicit number of microseconds for a request of one of the
  /// provided types to arrive. This tends to be used by servers.
  template <Message... Ms>
  RequestResult<Ms...> ReceiveWithTimeout(Duration timeout) {
    return implementation_.template Receive<Ms...>(address_, timeout);
  }

  /// Wait the default number of microseconds for a request of one of the
  /// provided types to arrive. This tends to be used by servers.
  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive() {
    const Duration timeout = default_timeout_;
    return implementation_.template Receive<Ms...>(address_, timeout);
  }

  /// Send a message in a best-effort fashion. This is used for messaging where
  /// responses are not necessarily expected, and for servers to respond to requests.
  /// If you need reliable delivery, this must be built on-top. TCP is not enough for most use cases.
  template <Message M>
  void Send(Address to_address, RequestId request_id, M message) {
    Address from_address = address_;
    return implementation_.template Send<M>(to_address, from_address, request_id, std::move(message));
  }

  /// The current system time. This time source should be preferred over any other,
  /// because it lets us deterministically control clocks from tests for making
  /// things like timeouts deterministic.
  Time Now() const { return implementation_.Now(); }

  /// Returns true if the system should shut-down.
  bool ShouldShutDown() const { return implementation_.ShouldShutDown(); }

  /// Returns a random number within the specified distribution.
  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    return implementation_.template Rand<D, Return>(distrib);
  }

  Address GetAddress() { return address_; }
  void SetAddress(Address address) { address_ = address; }

  Io<I> ForkLocal(boost::uuids::uuid uuid) {
    Address new_address{
        .unique_id = uuid,
        .last_known_ip = address_.last_known_ip,
        .last_known_port = address_.last_known_port,
    };
    return Io(implementation_, new_address);
  }

  LatencyHistogramSummaries ResponseLatencies() { return implementation_.ResponseLatencies(); }
};
};  // namespace memgraph::io
