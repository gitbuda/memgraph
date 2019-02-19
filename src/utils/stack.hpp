#pragma once

#include <experimental/optional>
#include <mutex>

#include <glog/logging.h>

#include "utils/linux.hpp"
#include "utils/spin_lock.hpp"

namespace utils {

/// This class implements a stack. It is primarily intended for storing
/// primitive types. This stack is thread-safe. It uses a spin lock to lock all
/// operations.
///
/// The stack stores all objects in memory blocks. That makes it really
/// efficient in terms of memory allocations. The memory block size is optimized
/// to be a multiple of the Linux memory page size so that only entire memory
/// pages are used. The constructor has a static_assert to warn you that you are
/// not using a valid `TSize` parameter. The `TSize` parameter should make the
/// `struct Block` a multiple in size of the Linux memory page size.
///
/// This can be calculated using:
/// sizeof(Block *) + sizeof(uint64_t) + sizeof(TObj) * TSize \
///     == k * kLinuxPageSize
///
/// Which translates to:
/// 8 + 8 + sizeof(TObj) * TSize == k * 4096
///
/// @tparam TObj primitive object that should be stored in the stack
/// @tparam TSize size of the memory block used
template <typename TObj, uint64_t TSize>
class Stack {
 private:
  struct Block {
    Block *prev{nullptr};
    uint64_t used{0};
    TObj obj[TSize];
  };

 public:
  Stack() {
    static_assert(sizeof(Block) % kLinuxPageSize == 0,
                  "It is recommended that you set the TSize constant so that "
                  "the size of Stack::Block is a multiple of the page size.");
  }

  Stack(Stack &&other) : head_(other.head_) { other.head_ = nullptr; }
  Stack &operator=(Stack &&other) {
    while (head_ != nullptr) {
      Block *prev = head_->prev;
      delete head_;
      head_ = prev;
    }
    head_ = other.head_;
    other.head_ = nullptr;
    return *this;
  }

  Stack(const Stack &) = delete;
  Stack &operator=(const Stack &) = delete;

  ~Stack() {
    while (head_ != nullptr) {
      Block *prev = head_->prev;
      delete head_;
      head_ = prev;
    }
  }

  void Push(TObj obj) {
    std::lock_guard<SpinLock> guard(lock_);
    if (head_ == nullptr) {
      // Allocate a new block.
      head_ = new Block();
    }
    while (true) {
      CHECK(head_->used <= TSize) << "utils::Stack has more elements in a "
                                     "Block than the block has space!";
      if (head_->used == TSize) {
        // Allocate a new block.
        Block *block = new Block();
        block->prev = head_;
        head_ = block;
      } else {
        head_->obj[head_->used++] = obj;
        break;
      }
    }
  }

  std::experimental::optional<TObj> Pop() {
    std::lock_guard<SpinLock> guard(lock_);
    while (true) {
      if (head_ == nullptr) return std::experimental::nullopt;
      CHECK(head_->used <= TSize) << "utils::Stack has more elements in a "
                                     "Block than the block has space!";
      if (head_->used == 0) {
        Block *prev = head_->prev;
        delete head_;
        head_ = prev;
      } else {
        return head_->obj[--head_->used];
      }
    }
  }

 private:
  SpinLock lock_;
  Block *head_{nullptr};
};

}  // namespace utils