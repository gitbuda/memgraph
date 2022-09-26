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

#include <algorithm>

#include "query/v2/plan/operator_coro.hpp"

#include "query/v2/interpret/eval.hpp"
#include "query/v2/interpret/multiframe.hpp"
#include "query/v2/plan/scoped_profile.hpp"
#include "utils/event_counter.hpp"
#include "utils/pmr/unordered_set.hpp"

// macro for the default implementation of LogicalOperator::Accept
// that accepts the visitor and visits it's input_ operator
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ACCEPT_WITH_INPUT(class_name)                                    \
  bool class_name::Accept(HierarchicalLogicalOperatorVisitor &visitor) { \
    if (visitor.PreVisit(*this)) {                                       \
      input_->Accept(visitor);                                           \
    }                                                                    \
    return visitor.PostVisit(*this);                                     \
  }

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define WITHOUT_SINGLE_INPUT(class_name)                         \
  bool class_name::HasSingleInput() const { return false; }      \
  std::shared_ptr<LogicalOperator> class_name::input() const {   \
    LOG_FATAL("Operator " #class_name " has no single input!");  \
  }                                                              \
  void class_name::set_input(std::shared_ptr<LogicalOperator>) { \
    LOG_FATAL("Operator " #class_name " has no single input!");  \
  }

namespace EventCounter {
extern const Event OnceOperator;
extern const Event ScanAllOperator;
extern const Event ProduceOperator;
}  // namespace EventCounter

namespace memgraph::query::v2::plan::coro {

template <class TCursor, class... TArgs>
std::unique_ptr<Cursor, std::function<void(Cursor *)>> MakeUniqueCursorPtr(utils::Allocator<TCursor> allocator,
                                                                           TArgs &&...args) {
  auto *ptr = allocator.allocate(1);
  try {
    auto *cursor = new (ptr) TCursor(std::forward<TArgs>(args)...);
    return std::unique_ptr<Cursor, std::function<void(Cursor *)>>(cursor, [allocator](Cursor *base_ptr) mutable {
      auto *p = static_cast<TCursor *>(base_ptr);
      p->~TCursor();
      allocator.deallocate(p, 1);
    });
  } catch (...) {
    allocator.deallocate(ptr, 1);
    throw;
  }
}

namespace {
template <typename T>
uint64_t ComputeProfilingKey(const T *obj) {
  static_assert(sizeof(T *) == sizeof(uint64_t));
  return reinterpret_cast<uint64_t>(obj);
}

// Custom equality function for a vector of typed values.
// Used in unordered_maps in Aggregate and Distinct operators.
struct TypedValueVectorEqual {
  template <class TAllocator>
  bool operator()(const std::vector<TypedValue, TAllocator> &left,
                  const std::vector<TypedValue, TAllocator> &right) const {
    MG_ASSERT(left.size() == right.size(),
              "TypedValueVector comparison should only be done over vectors "
              "of the same size");
    return std::equal(left.begin(), left.end(), right.begin(), TypedValue::BoolEqual{});
  }
};

// Returns boolean result of evaluating filter expression. Null is treated as
// false. Other non boolean values raise a QueryRuntimeException.
bool EvaluateFilter(ExpressionEvaluator &evaluator, Expression *filter) {
  TypedValue result = filter->Accept(evaluator);
  // Null is treated like false.
  if (result.IsNull()) return false;
  if (result.type() != TypedValue::Type::Bool)
    throw QueryRuntimeException("Filter expression must evaluate to bool or null, got {}.", result.type());
  return result.ValueBool();
}
}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SCOPED_PROFILE_OP(name) ScopedProfile profile{ComputeProfilingKey(this), name, &context};

SyncGenerator<bool> Once::OnceCursor::Pull(MultiFrame & /*frames*/, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Once");
  if (!did_pull_) {
    did_pull_ = true;
    co_yield true;
  }
  co_return false;
}

UniqueCursorPtr Once::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::OnceOperator);

  return MakeUniqueCursorPtr<OnceCursor>(mem);
}

WITHOUT_SINGLE_INPUT(Once);

void Once::OnceCursor::Shutdown() {}

void Once::OnceCursor::Reset() { did_pull_ = false; }

template <class TVerticesFun>
class ScanAllCursor : public Cursor {
 public:
  explicit ScanAllCursor(Symbol output_symbol, UniqueCursorPtr input_cursor, TVerticesFun get_vertices,
                         const char *op_name, bool perform_full_enumeration)
      : output_symbol_(output_symbol),
        input_cursor_(std::move(input_cursor)),
        get_vertices_(std::move(get_vertices)),
        op_name_(op_name),
        perform_full_enumeration_(perform_full_enumeration) {}

  SyncGenerator<bool> Pull(MultiFrame &multiframe, ExecutionContext &context) override {
    SCOPED_PROFILE_OP("ScanAll");
    auto gen = input_cursor_->Pull(multiframe, context);
    while (gen) {
      while (true) {
        auto next_vertices = get_vertices_(multiframe, context);
        if (!next_vertices) {
          break;
        }
        vertices_.emplace(std::move(next_vertices.value()));
        vertices_it_.emplace(vertices_.value().begin());
        while (vertices_it_.value() != vertices_.value().end()) {
          for (int i = 0; i < multiframe.Size(); ++i) {
            auto &frame = multiframe.GetFrame(i);
            frame[output_symbol_] = *vertices_it_.value();
            ++vertices_it_.value();
          }
          co_yield true;
        }
      }
    }
    co_return false;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    vertices_ = std::nullopt;
    vertices_it_ = std::nullopt;
  }

 private:
  const Symbol output_symbol_;
  const UniqueCursorPtr input_cursor_;
  TVerticesFun get_vertices_;
  std::optional<typename std::invoke_result<TVerticesFun, MultiFrame &, ExecutionContext &>::type::value_type>
      vertices_;
  std::optional<decltype(vertices_.value().begin())> vertices_it_;
  const char *op_name_;
  bool perform_full_enumeration_ = false;
};

ScanAll::ScanAll(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol, storage::v3::View view)
    : input_(input ? input : std::make_shared<Once>()),
      output_symbol_(output_symbol),
      view_(view),
      perform_full_enumeration_(false) {}

ACCEPT_WITH_INPUT(ScanAll)

UniqueCursorPtr ScanAll::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ScanAllOperator);
  auto vertices = [this](MultiFrame &, ExecutionContext &context) {
    auto *db = context.db_accessor;
    return std::make_optional(db->Vertices(view_));
  };
  return MakeUniqueCursorPtr<ScanAllCursor<decltype(vertices)>>(
      mem, output_symbol_, input_->MakeCursor(mem), std::move(vertices), "ScanAll", perform_full_enumeration_);
}

std::vector<Symbol> ScanAll::ModifiedSymbols(const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(output_symbol_);
  return symbols;
}

Produce::Produce(const std::shared_ptr<coro::LogicalOperator> &input,
                 const std::vector<NamedExpression *> &named_expressions)
    : input_(input ? input : std::make_shared<Once>()), named_expressions_(named_expressions) {}

ACCEPT_WITH_INPUT(Produce)

UniqueCursorPtr Produce::MakeCursor(utils::MemoryResource *mem) const {
  EventCounter::IncrementCounter(EventCounter::ProduceOperator);

  return MakeUniqueCursorPtr<ProduceCursor>(mem, *this, mem);
}

std::vector<Symbol> Produce::OutputSymbols(const SymbolTable &symbol_table) const {
  std::vector<Symbol> symbols;
  for (const auto *named_expr : named_expressions_) {
    symbols.emplace_back(symbol_table.at(*named_expr));
  }
  return symbols;
}

std::vector<Symbol> Produce::ModifiedSymbols(const SymbolTable &table) const { return OutputSymbols(table); }

Produce::ProduceCursor::ProduceCursor(const Produce &self, utils::MemoryResource *mem)
    : self_(self), input_cursor_(self_.input_->MakeCursor(mem)) {}

SyncGenerator<bool> Produce::ProduceCursor::Pull(MultiFrame &multiframe, ExecutionContext &context) {
  SCOPED_PROFILE_OP("Produce");
  auto gen = input_cursor_->Pull(multiframe, context);
  while (gen) {
    for (auto *frame : multiframe.GetFrames()) {
      if (!frame->IsValid()) {
        continue;
      }
      ExpressionEvaluator evaluator(frame, context.symbol_table, context.evaluation_context, context.db_accessor,
                                    storage::v3::View::NEW);
      for (auto *named_expr : self_.named_expressions_) {
        named_expr->Accept(evaluator);
      }
    }
    co_yield true;
  }
  co_return false;
}

void Produce::ProduceCursor::Shutdown() { input_cursor_->Shutdown(); }

void Produce::ProduceCursor::Reset() { input_cursor_->Reset(); }

}  // namespace memgraph::query::v2::plan::coro
