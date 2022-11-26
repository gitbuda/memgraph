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
#include <deque>
#include <memory>
#include <queue>
#include <variant>

#include "coordinator/coordinator.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "coordinator/shard_map.hpp"
#include "io/address.hpp"
#include "io/future.hpp"
#include "io/messages.hpp"
#include "io/rsm/raft.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"
#include "query/v2/requests.hpp"

namespace memgraph::coordinator::coordinator_worker {

/// Obligations:
/// * ShutDown
/// * Cron
/// * RouteMessage

using coordinator::Coordinator;
using coordinator::CoordinatorRsm;
using io::Address;
using io::RequestId;
using io::Time;
using io::messages::CoordinatorMessages;
using msgs::ReadRequests;
using msgs::ReadResponses;
using msgs::WriteRequests;
using msgs::WriteResponses;

struct ShutDown {};

struct Cron {};

struct RouteMessage {
  CoordinatorMessages message;
  RequestId request_id;
  Address to;
  Address from;
};

using Message = std::variant<RouteMessage, Cron, ShutDown>;

struct QueueInner {
  std::mutex mu{};
  std::condition_variable cv;
  // TODO(tyler) handle simulator communication std::shared_ptr<std::atomic<int>> blocked;

  // TODO(tyler) investigate using a priority queue that prioritizes messages in a way that
  // improves overall QoS. For example, maybe we want to schedule raft Append messages
  // ahead of Read messages or generally writes before reads for lowering the load on the
  // overall system faster etc... When we do this, we need to make sure to avoid
  // starvation by sometimes randomizing priorities, rather than following a strict
  // prioritization.
  std::deque<Message> queue;

  uint64_t submitted = 0;
  uint64_t calls_to_pop = 0;
};

/// There are two reasons to implement our own Queue instead of using
/// one off-the-shelf:
/// 1. we will need to know in the simulator when all threads are waiting
/// 2. we will want to implement our own priority queue within this for QoS
class Queue {
  std::shared_ptr<QueueInner> inner_ = std::make_shared<QueueInner>();

 public:
  void Push(Message &&message) {
    {
      MG_ASSERT(inner_.use_count() > 0);
      std::unique_lock<std::mutex> lock(inner_->mu);

      inner_->submitted++;

      inner_->queue.emplace_back(std::move(message));
    }  // lock dropped before notifying condition variable

    inner_->cv.notify_all();
  }

  Message Pop() {
    MG_ASSERT(inner_.use_count() > 0);
    std::unique_lock<std::mutex> lock(inner_->mu);

    inner_->calls_to_pop++;
    inner_->cv.notify_all();

    while (inner_->queue.empty()) {
      inner_->cv.wait(lock);
    }

    Message message = std::move(inner_->queue.front());
    inner_->queue.pop_front();

    return message;
  }

  void BlockOnQuiescence() const {
    MG_ASSERT(inner_.use_count() > 0);
    std::unique_lock<std::mutex> lock(inner_->mu);

    while (inner_->calls_to_pop <= inner_->submitted) {
      inner_->cv.wait(lock);
    }
  }
};

/// A CoordinatorWorker owns Raft<CoordinatorRsm> instances. receives messages from the MachineManager.
template <typename IoImpl>
class CoordinatorWorker {
  io::Io<IoImpl> io_;
  Queue queue_;
  CoordinatorRsm<IoImpl> coordinator_;

  bool Process(ShutDown && /*shut_down*/) { return false; }

  bool Process(Cron && /* cron */) {
    coordinator_.Cron();
    return true;
  }

  bool Process(RouteMessage &&route_message) {
    coordinator_.Handle(std::move(route_message.message), route_message.request_id, route_message.from);

    return true;
  }

 public:
  CoordinatorWorker(io::Io<IoImpl> io, Queue queue, Coordinator coordinator)
      : io_(std::move(io)), queue_(std::move(queue)), coordinator_{std::move(io_), {}, std::move(coordinator)} {}

  CoordinatorWorker(CoordinatorWorker &&) noexcept = default;
  CoordinatorWorker &operator=(CoordinatorWorker &&) noexcept = default;
  CoordinatorWorker(const CoordinatorWorker &) = delete;
  CoordinatorWorker &operator=(const CoordinatorWorker &) = delete;
  ~CoordinatorWorker() = default;

  void Run() {
    bool should_continue = true;
    while (should_continue) {
      Message message = queue_.Pop();

      should_continue = std::visit([this](auto &&msg) { return this->Process(std::forward<decltype(msg)>(msg)); },
                                   std::move(message));
    }
  }
};

}  // namespace memgraph::coordinator::coordinator_worker
