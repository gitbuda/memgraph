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

#include "query/v2/multiframe.hpp"

#include <algorithm>
#include <iterator>

#include "query/v2/bindings/frame.hpp"
#include "utils/pmr/vector.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(default_multi_frame_size, 100, "Default size of MultiFrame");

namespace memgraph::query::v2 {

static_assert(std::forward_iterator<ValidFramesReader::Iterator>);
static_assert(std::forward_iterator<ValidFramesModifier::Iterator>);
static_assert(std::forward_iterator<ValidFramesConsumer::Iterator>);
static_assert(std::forward_iterator<InvalidFramesPopulator::Iterator>);

MultiFrame::MultiFrame(size_t size_of_frame, size_t number_of_frames, utils::MemoryResource *execution_memory)
    : frames_(utils::pmr::vector<FrameWithValidity>(
          number_of_frames, FrameWithValidity(size_of_frame, execution_memory), execution_memory)) {
  MG_ASSERT(number_of_frames > 0);
}

MultiFrame::MultiFrame(const MultiFrame &other) : frames_{other.frames_} {}

// NOLINTNEXTLINE (bugprone-exception-escape)
MultiFrame::MultiFrame(MultiFrame &&other) noexcept : frames_(std::move(other.frames_)) {}

FrameWithValidity &MultiFrame::GetFirstFrame() {
  MG_ASSERT(!frames_.empty());
  return frames_.front();
}

void MultiFrame::MakeAllFramesInvalid() noexcept {
  std::for_each(frames_.begin(), frames_.end(), [](auto &frame) { frame.MakeInvalid(); });
}

bool MultiFrame::HasValidFrame() const noexcept {
  return std::any_of(frames_.begin(), frames_.end(), [](const auto &frame) { return frame.IsValid(); });
}

bool MultiFrame::HasInvalidFrame() const noexcept {
  return std::any_of(frames_.rbegin(), frames_.rend(), [](const auto &frame) { return !frame.IsValid(); });
}

// NOLINTNEXTLINE (bugprone-exception-escape)
void MultiFrame::DefragmentValidFrames() noexcept {
  static constexpr auto kIsValid = [](const FrameWithValidity &frame) { return frame.IsValid(); };
  static constexpr auto kIsInvalid = [](const FrameWithValidity &frame) { return !frame.IsValid(); };
  auto first_invalid_frame = std::find_if(frames_.begin(), frames_.end(), kIsInvalid);
  auto following_first_valid = std::find_if(first_invalid_frame, frames_.end(), kIsValid);
  while (first_invalid_frame != frames_.end() && following_first_valid != frames_.end()) {
    std::swap(*first_invalid_frame, *following_first_valid);
    first_invalid_frame++;
    first_invalid_frame = std::find_if(first_invalid_frame, frames_.end(), kIsInvalid);
    following_first_valid++;
    following_first_valid = std::find_if(following_first_valid, frames_.end(), kIsValid);
  }
}

ValidFramesReader MultiFrame::GetValidFramesReader() { return ValidFramesReader{*this}; }

ValidFramesModifier MultiFrame::GetValidFramesModifier() { return ValidFramesModifier{*this}; }

ValidFramesConsumer MultiFrame::GetValidFramesConsumer() { return ValidFramesConsumer{*this}; }

InvalidFramesPopulator MultiFrame::GetInvalidFramesPopulator() { return InvalidFramesPopulator{*this}; }

ValidFramesReader::ValidFramesReader(MultiFrame &multiframe) : multiframe_(&multiframe) {
  /*
  From: https://en.cppreference.com/w/cpp/algorithm/find
  Returns an iterator to the first element in the range [first, last) that satisfies specific criteria:
  find_if searches for an element for which predicate p returns true
  Return value
    Iterator to the first element satisfying the condition or last if no such element is found.

  -> this is what we want. We want the "after" last valid frame (weather this is vector::end or and invalid frame).
  */
  auto it = std::find_if(multiframe.frames_.begin(), multiframe.frames_.end(),
                         [](const auto &frame) { return !frame.IsValid(); });
  after_last_valid_frame_ = multiframe_->frames_.data() + std::distance(multiframe.frames_.begin(), it);
}

ValidFramesReader::Iterator ValidFramesReader::begin() {
  if (multiframe_->frames_[0].IsValid()) {
    return Iterator{&multiframe_->frames_[0]};
  }
  return end();
}

ValidFramesReader::Iterator ValidFramesReader::end() { return Iterator{after_last_valid_frame_}; }

ValidFramesModifier::ValidFramesModifier(MultiFrame &multiframe) : multiframe_(&multiframe) {}

ValidFramesModifier::Iterator ValidFramesModifier::begin() {
  if (multiframe_->frames_[0].IsValid()) {
    return Iterator{&multiframe_->frames_[0], *this};
  }
  return end();
}

ValidFramesModifier::Iterator ValidFramesModifier::end() {
  return Iterator{multiframe_->frames_.data() + multiframe_->frames_.size(), *this};
}

ValidFramesConsumer::ValidFramesConsumer(MultiFrame &multiframe) : multiframe_(&multiframe) {}

// NOLINTNEXTLINE (bugprone-exception-escape)
ValidFramesConsumer::~ValidFramesConsumer() noexcept {
  // TODO Possible optimisation: only DefragmentValidFrames if one frame has been invalidated? Only if does not
  // cost too much to store it
  multiframe_->DefragmentValidFrames();
}

ValidFramesConsumer::Iterator ValidFramesConsumer::begin() {
  if (multiframe_->frames_[0].IsValid()) {
    return Iterator{&multiframe_->frames_[0], *this};
  }
  return end();
}

ValidFramesConsumer::Iterator ValidFramesConsumer::end() {
  return Iterator{multiframe_->frames_.data() + multiframe_->frames_.size(), *this};
}

InvalidFramesPopulator::InvalidFramesPopulator(MultiFrame &multiframe) : multiframe_(&multiframe) {}

InvalidFramesPopulator::Iterator InvalidFramesPopulator::begin() {
  for (auto &frame : multiframe_->frames_) {
    if (!frame.IsValid()) {
      return Iterator{&frame};
    }
  }
  return end();
}

InvalidFramesPopulator::Iterator InvalidFramesPopulator::end() {
  return Iterator{multiframe_->frames_.data() + multiframe_->frames_.size()};
}

}  // namespace memgraph::query::v2