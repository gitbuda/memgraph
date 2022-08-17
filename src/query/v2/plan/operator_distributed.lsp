;; Copyright 2022 Memgraph Ltd.
;;
;; Use of this software is governed by the Business Source License
;; included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
;; License, and you may not use this file except in compliance with the Business Source License.
;;
;; As of the Change Date specified in that file, in accordance with
;; the Business Source License, use of this software will be governed
;; by the Apache License, Version 2.0, included in the file
;; licenses/APL.txt.

#>cpp
/** @file */

#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "query/v2/common.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/frontend/semantic/symbol.hpp"
#include "query/v2/typed_value.hpp"
#include "storage/v3/id_types.hpp"
#include "utils/bound.hpp"
#include "utils/fnv.hpp"
#include "utils/memory.hpp"
#include "utils/visitor.hpp"
#include "utils/logging.hpp"
cpp<#

(lcp:namespace memgraph)
(lcp:namespace query)
(lcp:namespace v2)

#>cpp
struct ExecutionContext;
class ExpressionEvaluator;
class Frame;
class SymbolTable;
cpp<#

(lcp:namespace plan)
(lcp:namespace distributed)

#>cpp
using Frames = std::vector<Frame*>;

/// Base class for iteration cursors of @c LogicalOperator classes.
///
/// Each @c LogicalOperator must produce a concrete @c Cursor, which provides
/// the iteration mechanism.
class Cursor {
 public:
  /// Run an iteration of a @c LogicalOperator.
  ///
  /// Since operators may be chained, the iteration may pull results from
  /// multiple operators.
  ///
  /// @param Frame May be read from or written to while performing the
  ///     iteration.
  /// @param ExecutionContext Used to get the position of symbols in frame and
  ///     other information.
  ///
  /// @throws QueryRuntimeException if something went wrong with execution
  virtual bool Pull(Frames &, ExecutionContext &) = 0;

  /// Resets the Cursor to its initial state.
  virtual void Reset() = 0;

  /// Perform cleanup which may throw an exception
  virtual void Shutdown() = 0;

  virtual ~Cursor() {}
};

/// unique_ptr to Cursor managed with a custom deleter.
/// This allows us to use utils::MemoryResource for allocation.
using UniqueCursorPtr = std::unique_ptr<Cursor, std::function<void(Cursor *)>>;

template <class TCursor, class... TArgs>
std::unique_ptr<Cursor, std::function<void(Cursor *)>> MakeUniqueCursorPtr(
    utils::Allocator<TCursor> allocator, TArgs &&... args) {
  auto *ptr = allocator.allocate(1);
  try {
    auto *cursor = new (ptr) TCursor(std::forward<TArgs>(args)...);
    return std::unique_ptr<Cursor, std::function<void(Cursor *)>>(
        cursor, [allocator](Cursor *base_ptr) mutable {
          auto *p = static_cast<TCursor *>(base_ptr);
          p->~TCursor();
          allocator.deallocate(p, 1);
        });
  } catch (...) {
    allocator.deallocate(ptr, 1);
    throw;
  }
}

class Once;
class CreateNode;
class CreateExpand;
class ScanAll;
class ScanAllByLabel;
class ScanAllByLabelPropertyRange;
class ScanAllByLabelPropertyValue;
class ScanAllByLabelProperty;
class ScanAllById;
class Expand;
class ExpandVariable;
class ConstructNamedPath;
class Filter;
class Produce;
class Delete;
class SetProperty;
class SetProperties;
class SetLabels;
class RemoveProperty;
class RemoveLabels;
class EdgeUniquenessFilter;
class Accumulate;
class Aggregate;
class Skip;
class Limit;
class OrderBy;
class Merge;
class Optional;
class Unwind;
class Distinct;
class Union;
class Cartesian;
class CallProcedure;
class LoadCsv;
class Foreach;

using LogicalOperatorCompositeVisitor = utils::CompositeVisitor<
    Once, CreateNode, CreateExpand, ScanAll, ScanAllByLabel,
    ScanAllByLabelPropertyRange, ScanAllByLabelPropertyValue,
    ScanAllByLabelProperty, ScanAllById,
    Expand, ExpandVariable, ConstructNamedPath, Filter, Produce, Delete,
    SetProperty, SetProperties, SetLabels, RemoveProperty, RemoveLabels,
    EdgeUniquenessFilter, Accumulate, Aggregate, Skip, Limit, OrderBy, Merge,
    Optional, Unwind, Distinct, Union, Cartesian, CallProcedure, LoadCsv, Foreach>;

using LogicalOperatorLeafVisitor = utils::LeafVisitor<Once>;

/**
 * @brief Base class for hierarhical visitors of @c LogicalOperator class
 * hierarchy.
 */
class HierarchicalLogicalOperatorVisitor
    : public LogicalOperatorCompositeVisitor,
      public LogicalOperatorLeafVisitor {
 public:
  using LogicalOperatorCompositeVisitor::PostVisit;
  using LogicalOperatorCompositeVisitor::PreVisit;
  using LogicalOperatorLeafVisitor::Visit;
  using typename LogicalOperatorLeafVisitor::ReturnType;
};
cpp<#

(lcp:define-class logical-operator ("utils::Visitable<HierarchicalLogicalOperatorVisitor>")
  ()
  (:abstractp t)
  (:documentation
   "Base class for logical operators.

Each operator describes an operation, which is to be performed on the
database. Operators are iterated over using a @c Cursor. Various operators
can serve as inputs to others and thus a sequence of operations is formed.")
  (:public
   #>cpp
   virtual ~LogicalOperator() {}

   /** Construct a @c Cursor which is used to run this operator.
    *
    * @param utils::MemoryResource Memory resource used for allocations during
    *     the lifetime of the returned Cursor.
    */
   virtual UniqueCursorPtr MakeCursor(utils::MemoryResource *) const = 0;

   /** Return @c Symbol vector where the query results will be stored.
    *
    * Currently, output symbols are generated in @c Produce @c Union and
    * @c CallProcedure operators. @c Skip, @c Limit, @c OrderBy and @c Distinct
    * propagate the symbols from @c Produce (if it exists as input operator).
    *
    *  @param SymbolTable used to find symbols for expressions.
    *  @return std::vector<Symbol> used for results.
    */
   virtual std::vector<Symbol> OutputSymbols(const SymbolTable &) const {
     return std::vector<Symbol>();
   }

   /**
    * Symbol vector whose values are modified by this operator sub-tree.
    *
    * This is different than @c OutputSymbols, because it returns all of the
    * modified symbols, including those that may not be returned as the
    * result of the query. Note that the modified symbols will not contain
    * those that should not be read after the operator is processed.
    *
    * For example, `MATCH (n)-[e]-(m) RETURN n AS l` will generate `ScanAll (n) >
    * Expand (e, m) > Produce (l)`. The modified symbols on Produce sub-tree will
    * be `l`, the same as output symbols, because it isn't valid to read `n`, `e`
    * nor `m` after Produce. On the other hand, modified symbols from Expand
    * contain `e` and `m`, as well as `n`, while output symbols are empty.
    * Modified symbols from ScanAll contain only `n`, while output symbols are
    * also empty.
    */
   virtual std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const = 0;

   /**
    * Returns true if the operator takes only one input operator.
    * NOTE: When this method returns true, you may use `input` and `set_input`
    * methods.
    */
   virtual bool HasSingleInput() const = 0;

   /**
    * Returns the input operator if it has any.
    * NOTE: This should only be called if `HasSingleInput() == true`.
    */
   virtual std::shared_ptr<LogicalOperator> input() const = 0;
   /**
    * Set a different input on this operator.
    * NOTE: This should only be called if `HasSingleInput() == true`.
    */
   virtual void set_input(std::shared_ptr<LogicalOperator>) = 0;

   struct SaveHelper {
     std::vector<LogicalOperator *> saved_ops;
   };

   struct LoadHelper {
     AstStorage ast_storage;
     std::vector<std::pair<uint64_t, std::shared_ptr<LogicalOperator>>>
         loaded_ops;
   };

   struct SlkLoadHelper {
     AstStorage ast_storage;
     std::vector<std::shared_ptr<LogicalOperator>> loaded_ops;
   };
   cpp<#)
  (:serialize
   (:slk :base t
         :save-args '((helper "query::v2::plan::LogicalOperator::SaveHelper *"))
         :load-args '((helper "query::v2::plan::LogicalOperator::SlkLoadHelper *"))))
  (:type-info :base t)
  (:clone :args '((storage "AstStorage *"))
          :base t))

(defun slk-save-ast-pointer (member)
  #>cpp
  query::v2::SaveAstPointer(self.${member}, builder);
  cpp<#)

(defun slk-load-ast-pointer (type)
  (lambda (member)
    #>cpp
    self->${member} = query::v2::LoadAstPointer<query::v2::${type}>(
        &helper->ast_storage, reader);
    cpp<#))

(defun slk-save-ast-vector (member)
  #>cpp
  size_t size = self.${member}.size();
  slk::Save(size, builder);
  for (const auto *val : self.${member}) {
    query::v2::SaveAstPointer(val, builder);
  }
  cpp<#)

(defun slk-load-ast-vector (type)
  (lambda (member)
    #>cpp
    size_t size = 0;
    slk::Load(&size, reader);
    self->${member}.resize(size);
    for (size_t i = 0;
         i < size;
         ++i) {
      self->${member}[i] = query::v2::LoadAstPointer<query::v2::${type}>(
          &helper->ast_storage, reader);
    }
    cpp<#))

(defun slk-save-operator-pointer (member)
  #>cpp
  slk::Save<query::v2::plan::LogicalOperator>(self.${member}, builder,
                                          &helper->saved_ops,
                                          [&helper](const auto &val,
                                                    auto *builder) {
                                            slk::Save(val, builder, helper);
                                          });
  cpp<#)

(defun slk-load-operator-pointer (member)
  #>cpp
  slk::Load<query::v2::plan::LogicalOperator>(&self->${member}, reader, &helper->loaded_ops,
      [&helper](auto *op, auto *reader) {
        slk::ConstructAndLoad(op, reader, helper);
      });
  cpp<#)

(lcp:define-class once (logical-operator)
  ((symbols "std::vector<Symbol>" :scope :public))
  (:documentation
   "A logical operator whose Cursor returns true on the first Pull
and false on every following Pull.")
  (:public
   #>cpp
   Once(std::vector<Symbol> symbols = {}) : symbols_{std::move(symbols)} {}
   DEFVISITABLE(HierarchicalLogicalOperatorVisitor);
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override {
     return symbols_;
   }

   bool HasSingleInput() const override;
   std::shared_ptr<LogicalOperator> input() const override;
   void set_input(std::shared_ptr<LogicalOperator>) override;
   cpp<#)
  (:private
   #>cpp
   class OnceCursor : public Cursor {
    public:
     OnceCursor() {}
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     bool did_pull_{false};
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(defun slk-save-properties (member)
  #>cpp
  size_t size = self.${member}.size();
  slk::Save(size, builder);
  for (const auto &kv : self.${member}) {
    slk::Save(kv.first, builder);
    query::v2::SaveAstPointer(kv.second, builder);
  }
  cpp<#)

(defun slk-load-properties (member)
  #>cpp
  size_t size = 0;
  slk::Load(&size, reader);
  self->${member}.resize(size);
  for (size_t i = 0; i < size; ++i) {
    storage::v3::PropertyId prop;
    slk::Load(&prop, reader);
    auto *expr = query::v2::LoadAstPointer<query::v2::Expression>(
        &helper->ast_storage, reader);
    self->${member}[i] = {prop, expr};
  }
  cpp<#)

(defun clone-variant-properties (source destination)
  #>cpp
    if (const auto *props = std::get_if<PropertiesMapList>(&${source})) {
      auto &destination_props = std::get<PropertiesMapList>(${destination});
      destination_props.resize(props->size());
      for (auto i0 = 0; i0 < props->size(); ++i0) {
        {
          storage::v3::PropertyId first1 = (*props)[i0].first;
          Expression *second2;
          second2 = (*props)[i0].second ? (*props)[i0].second->Clone(storage) : nullptr;
          destination_props[i0] = std::make_pair(std::move(first1), std::move(second2));
        }
      }
    } else {
      ${destination} = std::get<ParameterLookup *>(${source})->Clone(storage);
    }
  cpp<#)

#>cpp
using PropertiesMapList = std::vector<std::pair<storage::v3::PropertyId, Expression *>>;
cpp<#

(lcp:define-struct node-creation-info ()
  ((symbol "Symbol")
   (labels "std::vector<storage::v3::LabelId>")
   (properties "std::variant<PropertiesMapList, ParameterLookup *>"
               :slk-save #'slk-save-properties
               :slk-load #'slk-load-properties
               :clone #'clone-variant-properties))
  (:serialize (:slk :save-args '((helper "query::v2::plan::LogicalOperator::SaveHelper *"))
                    :load-args '((helper "query::v2::plan::LogicalOperator::SlkLoadHelper *"))))
  (:clone :args '((storage "AstStorage *")))
  (:public
   #>cpp
  NodeCreationInfo() = default;

  NodeCreationInfo(
      Symbol symbol, std::vector<storage::v3::LabelId> labels,
      std::variant<PropertiesMapList, ParameterLookup *> properties)
      : symbol{std::move(symbol)}, labels{std::move(labels)}, properties{std::move(properties)} {};

  NodeCreationInfo(Symbol symbol, std::vector<storage::v3::LabelId> labels,
                    PropertiesMapList properties)
      : symbol{std::move(symbol)}, labels{std::move(labels)}, properties{std::move(properties)} {};

  NodeCreationInfo(Symbol symbol, std::vector<storage::v3::LabelId> labels, ParameterLookup* properties)
      : symbol{std::move(symbol)}, labels{std::move(labels)}, properties{properties} {};
  cpp<#))

(lcp:define-class create-node (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (node-info "NodeCreationInfo" :scope :public
              :slk-save (lambda (m)
                          #>cpp
                          slk::Save(self.${m}, builder, helper);
                          cpp<#)
              :slk-load (lambda (m)
                          #>cpp
                          slk::Load(&self->${m}, reader, helper);
                          cpp<#)))
  (:documentation
   "Operator for creating a node.

This op is used both for creating a single node (`CREATE` statement without
a preceding `MATCH`), or multiple nodes (`MATCH ... CREATE` or
`CREATE (), () ...`).

@sa CreateExpand")
  (:public
   #>cpp
   CreateNode() {}

   /**
    * @param input Optional. If @c nullptr, then a single node will be
    *    created (a single successful @c Cursor::Pull from this op's @c Cursor).
    *    If a valid input, then a node will be created for each
    *    successful pull from the given input.
    * @param node_info @c NodeCreationInfo
    */
   CreateNode(const std::shared_ptr<LogicalOperator> &input,
              const NodeCreationInfo &node_info);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class CreateNodeCursor : public Cursor {
    public:
     CreateNodeCursor(const CreateNode &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const CreateNode &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-struct edge-creation-info ()
  ((symbol "Symbol")
   (properties "std::variant<PropertiesMapList, ParameterLookup *>"
               :slk-save #'slk-save-properties
               :slk-load #'slk-load-properties
               :clone #'clone-variant-properties)
   (edge-type "::storage::v3::EdgeTypeId")
   (direction "::EdgeAtom::Direction" :initval "EdgeAtom::Direction::BOTH"))
  (:serialize (:slk :save-args '((helper "query::v2::plan::LogicalOperator::SaveHelper *"))
                    :load-args '((helper "query::v2::plan::LogicalOperator::SlkLoadHelper *"))))
  (:clone :args '((storage "AstStorage *")))
  (:public
   #>cpp
    EdgeCreationInfo() = default;

    EdgeCreationInfo(Symbol symbol, std::variant<PropertiesMapList, ParameterLookup *> properties,
      storage::v3::EdgeTypeId edge_type, EdgeAtom::Direction direction)
    : symbol{std::move(symbol)}, properties{std::move(properties)}, edge_type{edge_type}, direction{direction} {};

    EdgeCreationInfo(Symbol symbol, PropertiesMapList properties,
      storage::v3::EdgeTypeId edge_type, EdgeAtom::Direction direction)
    : symbol{std::move(symbol)}, properties{std::move(properties)}, edge_type{edge_type}, direction{direction} {};

    EdgeCreationInfo(Symbol symbol, ParameterLookup* properties,
      storage::v3::EdgeTypeId edge_type, EdgeAtom::Direction direction)
    : symbol{std::move(symbol)}, properties{properties}, edge_type{edge_type}, direction{direction} {};
  cpp<#))

(lcp:define-class create-expand (logical-operator)
  ((node-info "NodeCreationInfo" :scope :public
              :slk-save (lambda (m)
                          #>cpp
                          slk::Save(self.${m}, builder, helper);
                          cpp<#)
              :slk-load (lambda (m)
                          #>cpp
                          slk::Load(&self->${m}, reader, helper);
                          cpp<#))
   (edge-info "EdgeCreationInfo" :scope :public
              :slk-save (lambda (m)
                          #>cpp
                          slk::Save(self.${m}, builder, helper);
                          cpp<#)
              :slk-load (lambda (m)
                          #>cpp
                          slk::Load(&self->${m}, reader, helper);
                          cpp<#))
   ;; the input op and the symbol under which the op's result
   ;; can be found in the frame
   (input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (input-symbol "Symbol" :scope :public)
   (existing-node :bool :scope :public :documentation
                  "if the given node atom refers to an existing node (either matched or created)"))
  (:documentation
   "Operator for creating edges and destination nodes.

This operator extends already created nodes with an edge. If the other node
on the edge does not exist, it will be created. For example, in `MATCH (n)
CREATE (n) -[r:r]-> (n)` query, this operator will create just the edge `r`.
In `MATCH (n) CREATE (n) -[r:r]-> (m)` query, the operator will create both
the edge `r` and the node `m`. In case of `CREATE (n) -[r:r]-> (m)` the
first node `n` is created by @c CreateNode operator, while @c CreateExpand
will create the edge `r` and `m`. Similarly, multiple @c CreateExpand are
chained in cases when longer paths need creating.

@sa CreateNode")
  (:public
   #>cpp
   CreateExpand() {}

   /** @brief Construct @c CreateExpand.
    *
    * @param node_info @c NodeCreationInfo at the end of the edge.
    *     Used to create a node, unless it refers to an existing one.
    * @param edge_info @c EdgeCreationInfo for the edge to be created.
    * @param input Optional. Previous @c LogicalOperator which will be pulled.
    *     For each successful @c Cursor::Pull, this operator will create an
    *     expansion.
    * @param input_symbol @c Symbol for the node at the start of the edge.
    * @param existing_node @c bool indicating whether the @c node_atom refers to
    *     an existing node. If @c false, the operator will also create the node.
    */
   CreateExpand(const NodeCreationInfo &node_info,
                const EdgeCreationInfo &edge_info,
                const std::shared_ptr<LogicalOperator> &input,
                Symbol input_symbol, bool existing_node);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class CreateExpandCursor : public Cursor {
    public:
     CreateExpandCursor(const CreateExpand &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const CreateExpand &self_;
     const UniqueCursorPtr input_cursor_;

     // Get the existing node (if existing_node_ == true), or create a new node
     VertexAccessor &OtherVertex(Frames &frames, ExecutionContext &context);
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class scan-all (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (output-symbol "Symbol" :scope :public)
   (view "::storage::v3::View" :scope :public
         :documentation
         "Controls which graph state is used to produce vertices.

If @c storage::v3::View::OLD, @c ScanAll will produce vertices visible in the
previous graph state, before modifications done by current transaction &
command. With @c storage::v3::View::NEW, all vertices will be produced the current
transaction sees along with their modifications."))

  (:documentation
   "Operator which iterates over all the nodes currently in the database.
When given an input (optional), does a cartesian product.

It accepts an optional input. If provided then this op scans all the nodes
currently in the database for each successful Pull from it's input, thereby
producing a cartesian product of input Pulls and database elements.

ScanAll can either iterate over the previous graph state (state before
the current transacton+command) or over current state. This is controlled
with a constructor argument.

@sa ScanAllByLabel
@sa ScanAllByLabelPropertyRange
@sa ScanAllByLabelPropertyValue")
  (:public
   #>cpp
   ScanAll() {}
   ScanAll(const std::shared_ptr<LogicalOperator> &input, Symbol output_symbol,
           storage::v3::View view = storage::v3::View::OLD);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class scan-all-by-label (scan-all)
  ((label "::storage::v3::LabelId" :scope :public))
  (:documentation
   "Behaves like @c ScanAll, but this operator produces only vertices with
given label.

@sa ScanAll
@sa ScanAllByLabelPropertyRange
@sa ScanAllByLabelPropertyValue")
  (:public
   #>cpp
   ScanAllByLabel() {}
   ScanAllByLabel(const std::shared_ptr<LogicalOperator> &input,
                  Symbol output_symbol, storage::v3::LabelId label,
                  storage::v3::View view = storage::v3::View::OLD);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   cpp<#)
  (:serialize (:slk))
  (:clone))

(defun slk-save-optional-bound (member)
  #>cpp
  slk::Save(static_cast<bool>(self.${member}), builder);
  if (!self.${member}) {
    return;
  }
  uint8_t bound_type;
  const auto &bound = *self.${member};
  switch (bound.type()) {
    case utils::BoundType::INCLUSIVE:
      bound_type = 0;
      break;
    case utils::BoundType::EXCLUSIVE:
      bound_type = 1;
      break;
  }
  slk::Save(bound_type, builder);
  query::v2::SaveAstPointer(bound.value(), builder);
  cpp<#)

(defun slk-load-optional-bound (member)
  #>cpp
  bool has_bound;
  slk::Load(&has_bound, reader);
  if (!has_bound) {
    self->${member} = std::nullopt;
    return;
  }
  uint8_t bound_type_value;
  slk::Load(&bound_type_value, reader);
  utils::BoundType bound_type;
  switch (bound_type_value) {
    case static_cast<uint8_t>(0):
      bound_type = utils::BoundType::INCLUSIVE;
      break;
    case static_cast<uint8_t>(1):
      bound_type = utils::BoundType::EXCLUSIVE;
      break;
    default:
      throw slk::SlkDecodeException("Loading unknown BoundType");
  }
  auto *value = query::v2::LoadAstPointer<query::v2::Expression>(
      &helper->ast_storage, reader);
  self->${member}.emplace(utils::Bound<query::v2::Expression *>(value, bound_type));
  cpp<#)

(defun clone-optional-bound (source dest)
  #>cpp
  if (${source}) {
    ${dest}.emplace(utils::Bound<Expression *>(
                    ${source}->value()->Clone(storage),
                    ${source}->type()));
  } else {
    ${dest} = std::nullopt;
  }
  cpp<#)

(lcp:define-class scan-all-by-label-property-range (scan-all)
  ((label "::storage::v3::LabelId" :scope :public)
   (property "::storage::v3::PropertyId" :scope :public)
   (property-name "std::string" :scope :public)
   (lower-bound "std::optional<Bound>" :scope :public
                :slk-save #'slk-save-optional-bound
                :slk-load #'slk-load-optional-bound
                :clone #'clone-optional-bound)
   (upper-bound "std::optional<Bound>" :scope :public
                :slk-save #'slk-save-optional-bound
                :slk-load #'slk-load-optional-bound
                :clone #'clone-optional-bound))
  (:documentation
   "Behaves like @c ScanAll, but produces only vertices with given label and
property value which is inside a range (inclusive or exlusive).

@sa ScanAll
@sa ScanAllByLabel
@sa ScanAllByLabelPropertyValue")
  (:public
   #>cpp
   /** Bound with expression which when evaluated produces the bound value. */
   using Bound = utils::Bound<Expression *>;
   ScanAllByLabelPropertyRange() {}
   /**
    * Constructs the operator for given label and property value in range
    * (inclusive).
    *
    * Range bounds are optional, but only one bound can be left out.
    *
    * @param input Preceding operator which will serve as the input.
    * @param output_symbol Symbol where the vertices will be stored.
    * @param label Label which the vertex must have.
    * @param property Property from which the value will be looked up from.
    * @param lower_bound Optional lower @c Bound.
    * @param upper_bound Optional upper @c Bound.
    * @param view storage::v3::View used when obtaining vertices.
    */
   ScanAllByLabelPropertyRange(const std::shared_ptr<LogicalOperator> &input,
                               Symbol output_symbol, storage::v3::LabelId label,
                               storage::v3::PropertyId property,
                               const std::string &property_name,
                               std::optional<Bound> lower_bound,
                               std::optional<Bound> upper_bound,
                               storage::v3::View view = storage::v3::View::OLD);

   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class scan-all-by-label-property-value (scan-all)
  ((label "::storage::v3::LabelId" :scope :public)
   (property "::storage::v3::PropertyId" :scope :public)
   (property-name "std::string" :scope :public)
   (expression "Expression *" :scope :public
               :slk-save #'slk-save-ast-pointer
               :slk-load (slk-load-ast-pointer "Expression")))
  (:documentation
   "Behaves like @c ScanAll, but produces only vertices with given label and
property value.

@sa ScanAll
@sa ScanAllByLabel
@sa ScanAllByLabelPropertyRange")
  (:public
   #>cpp
   ScanAllByLabelPropertyValue() {}
   /**
    * Constructs the operator for given label and property value.
    *
    * @param input Preceding operator which will serve as the input.
    * @param output_symbol Symbol where the vertices will be stored.
    * @param label Label which the vertex must have.
    * @param property Property from which the value will be looked up from.
    * @param expression Expression producing the value of the vertex property.
    * @param view storage::v3::View used when obtaining vertices.
    */
   ScanAllByLabelPropertyValue(const std::shared_ptr<LogicalOperator> &input,
                               Symbol output_symbol, storage::v3::LabelId label,
                               storage::v3::PropertyId property,
                               const std::string &property_name,
                               Expression *expression,
                               storage::v3::View view = storage::v3::View::OLD);

   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class scan-all-by-label-property (scan-all)
  ((label "::storage::v3::LabelId" :scope :public)
   (property "::storage::v3::PropertyId" :scope :public)
   (property-name "std::string" :scope :public)
   (expression "Expression *" :scope :public
               :slk-save #'slk-save-ast-pointer
               :slk-load (slk-load-ast-pointer "Expression")))

  (:documentation
   "Behaves like @c ScanAll, but this operator produces only vertices with
given label and property.

@sa ScanAll
@sa ScanAllByLabelPropertyRange
@sa ScanAllByLabelPropertyValue")
  (:public
   #>cpp
   ScanAllByLabelProperty() {}
   ScanAllByLabelProperty(const std::shared_ptr<LogicalOperator> &input,
                          Symbol output_symbol, storage::v3::LabelId label,
                          storage::v3::PropertyId property,
                          const std::string &property_name,
                          storage::v3::View view = storage::v3::View::OLD);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   cpp<#)
  (:serialize (:slk))
  (:clone))



(lcp:define-class scan-all-by-id (scan-all)
  ((expression "Expression *" :scope :public
               :slk-save #'slk-save-ast-pointer
               :slk-load (slk-load-ast-pointer "Expression")))
  (:documentation
    "ScanAll producing a single node with ID equal to evaluated expression")
  (:public
    #>cpp
    ScanAllById() {}
    ScanAllById(const std::shared_ptr<LogicalOperator> &input,
                Symbol output_symbol, Expression *expression,
                storage::v3::View view = storage::v3::View::OLD);

    bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
    UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
    cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-struct expand-common ()
  (
   ;; info on what's getting expanded
   (node-symbol "Symbol"
                :documentation "Symbol pointing to the node to be expanded.
This is where the new node will be stored.")
   (edge-symbol "Symbol"
                :documentation "Symbol for the edges to be expanded.
This is where a TypedValue containing a list of expanded edges will be stored.")
   (direction "::EdgeAtom::Direction"
              :documentation "EdgeAtom::Direction determining the direction of edge
expansion. The direction is relative to the starting vertex for each expansion.")
   (edge-types "std::vector<storage::v3::EdgeTypeId>"
               :documentation "storage::v3::EdgeTypeId specifying which edges we want
to expand. If empty, all edges are valid. If not empty, only edges with one of
the given types are valid.")
   (existing-node :bool :documentation "If the given node atom refer to a symbol
that has already been expanded and should be just validated in the frame."))
  (:serialize (:slk)))

(lcp:define-class expand (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (input-symbol "Symbol" :scope :public)
   (common "ExpandCommon" :scope :public)
   (view "::storage::v3::View" :scope :public
         :documentation
         "State from which the input node should get expanded."))
  (:documentation
   "Expansion operator. For a node existing in the frame it
expands one edge and one node and places them on the frame.

This class does not handle node/edge filtering based on
properties, labels and edge types. However, it does handle
filtering on existing node / edge.

Filtering on existing means that for a pattern that references
an already declared node or edge (for example in
MATCH (a) MATCH (a)--(b)),
only expansions that match defined equalities are successfully
pulled.")
  (:public
   #>cpp
   /**
    * Creates an expansion. All parameters except input and input_symbol are
    * forwarded to @c ExpandCommon and are documented there.
    *
    * @param input Optional logical operator that preceeds this one.
    * @param input_symbol Symbol that points to a VertexAccessor in the frame
    *    that expansion should emanate from.
    */
   Expand(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
          Symbol node_symbol, Symbol edge_symbol, EdgeAtom::Direction direction,
          const std::vector<storage::v3::EdgeTypeId> &edge_types, bool existing_node,
          storage::v3::View view);

   Expand() {}

   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }

   class ExpandCursor : public Cursor {
    public:
     ExpandCursor(const Expand &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     using InEdgeT = std::remove_reference_t<decltype(
         *std::declval<VertexAccessor>().InEdges(storage::v3::View::OLD))>;
     using InEdgeIteratorT = decltype(std::declval<InEdgeT>().begin());
     using OutEdgeT = std::remove_reference_t<decltype(
         *std::declval<VertexAccessor>().OutEdges(storage::v3::View::OLD))>;
     using OutEdgeIteratorT = decltype(std::declval<OutEdgeT>().begin());

     const Expand &self_;
     const UniqueCursorPtr input_cursor_;

     // The iterable over edges and the current edge iterator are referenced via
     // optional because they can not be initialized in the constructor of
     // this class. They are initialized once for each pull from the input.
     std::optional<InEdgeT> in_edges_;
     std::optional<InEdgeIteratorT> in_edges_it_;
     std::optional<OutEdgeT> out_edges_;
     std::optional<OutEdgeIteratorT> out_edges_it_;

     bool InitEdges(Frames &, ExecutionContext &);
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-struct expansion-lambda ()
  ((inner-edge-symbol "Symbol" :documentation "Currently expanded edge symbol.")
   (inner-node-symbol "Symbol" :documentation "Currently expanded node symbol.")
   (expression "Expression *" :documentation "Expression used in lambda during expansion."
               :slk-save #'slk-save-ast-pointer
               :slk-load (lambda (member)
                           #>cpp
                           self->${member} = query::v2::LoadAstPointer<query::v2::Expression>(
                               ast_storage, reader);
                           cpp<#)))
  (:serialize (:slk :load-args '((ast-storage "query::v2::AstStorage *"))))
  (:clone :args '((storage "AstStorage *"))))

(lcp:define-class expand-variable (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (input-symbol "Symbol" :scope :public)
   (common "ExpandCommon" :scope :public)
   (type "::EdgeAtom::Type" :scope :public)
   (is-reverse :bool :scope :public :documentation
               "True if the path should be written as expanding from node_symbol to input_symbol.")
   (lower-bound "Expression *" :scope :public
                :slk-save #'slk-save-ast-pointer
                :slk-load (slk-load-ast-pointer "Expression")
                :documentation "Optional lower bound of the variable length expansion, defaults are (1, inf)")
   (upper-bound "Expression *" :scope :public
                :slk-save #'slk-save-ast-pointer
                :slk-load (slk-load-ast-pointer "Expression")
                :documentation "Optional upper bound of the variable length expansion, defaults are (1, inf)")
   (filter-lambda "ExpansionLambda"
                  :scope :public
                  :slk-load (lambda (member)
                              #>cpp
                              slk::Load(&self->${member}, reader, &helper->ast_storage);
                              cpp<#))
   (weight-lambda "std::optional<ExpansionLambda>" :scope :public
                  :slk-load (lambda (member)
                              #>cpp
                              bool has_value;
                              slk::Load(&has_value, reader);
                              if (!has_value) {
                                self->${member} = std::nullopt;
                                return;
                              }
                              query::v2::plan::ExpansionLambda lambda;
                              slk::Load(&lambda, reader, &helper->ast_storage);
                              self->${member}.emplace(lambda);
                              cpp<#))
   (total-weight "std::optional<Symbol>" :scope :public))
  (:documentation
   "Variable-length expansion operator. For a node existing in
the frame it expands a variable number of edges and places them
(in a list-type TypedValue), as well as the final destination node,
on the frame.

This class does not handle node/edge filtering based on
properties, labels and edge types. However, it does handle
filtering on existing node / edge. Additionally it handles's
edge-uniquess (cyphermorphism) because it's not feasable to do
later.

Filtering on existing means that for a pattern that references
an already declared node or edge (for example in
MATCH (a) MATCH (a)--(b)),
only expansions that match defined equalities are succesfully
pulled.")
  (:public
   #>cpp
   ExpandVariable() {}

   /**
    * Creates a variable-length expansion. Most params are forwarded
    * to the @c ExpandCommon constructor, and are documented there.
    *
    * Expansion length bounds are both inclusive (as in Neo's Cypher
    * implementation).
    *
    * @param input Optional logical operator that preceeds this one.
    * @param input_symbol Symbol that points to a VertexAccessor in the frame
    *    that expansion should emanate from.
    * @param type - Either Type::DEPTH_FIRST (default variable-length expansion),
    * or Type::BREADTH_FIRST.
    * @param is_reverse Set to `true` if the edges written on frame should expand
    *    from `node_symbol` to `input_symbol`. Opposed to the usual expanding
    *    from `input_symbol` to `node_symbol`.
    * @param lower_bound An optional indicator of the minimum number of edges
    *    that get expanded (inclusive).
    * @param upper_bound An optional indicator of the maximum number of edges
    *    that get expanded (inclusive).
    * @param inner_edge_symbol Like `inner_node_symbol`
    * @param inner_node_symbol For each expansion the node expanded into is
    *    assigned to this symbol so it can be evaulated by the 'where'
    * expression.
    * @param filter_ The filter that must be satisfied for an expansion to
    * succeed. Can use inner(node/edge) symbols. If nullptr, it is ignored.
    */
    ExpandVariable(const std::shared_ptr<LogicalOperator> &input,
                   Symbol input_symbol, Symbol node_symbol, Symbol edge_symbol,
                   EdgeAtom::Type type, EdgeAtom::Direction direction,
                   const std::vector<storage::v3::EdgeTypeId> &edge_types,
                   bool is_reverse, Expression *lower_bound,
                   Expression *upper_bound, bool existing_node,
                   ExpansionLambda filter_lambda,
                   std::optional<ExpansionLambda> weight_lambda,
                   std::optional<Symbol> total_weight);

   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   // the Cursors are not declared in the header because
   // it's edges_ and edges_it_ are decltyped using a helper function
   // that should be inaccessible (private class function won't compile)
   friend class ExpandVariableCursor;
   friend class ExpandWeightedShortestPathCursor;
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class construct-named-path (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (path-symbol "Symbol" :scope :public)
   (path-elements "std::vector<Symbol>" :scope :public))
  (:documentation
   "Constructs a named path from its elements and places it on the frame.")
  (:public
   #>cpp
   ConstructNamedPath() {}
   ConstructNamedPath(const std::shared_ptr<LogicalOperator> &input,
                      Symbol path_symbol,
                      const std::vector<Symbol> &path_elements)
       : input_(input),
         path_symbol_(path_symbol),
         path_elements_(path_elements) {}
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class filter (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (expression "Expression *" :scope :public
               :slk-save #'slk-save-ast-pointer
               :slk-load (slk-load-ast-pointer "Expression")))
  (:documentation
   "Filter whose Pull returns true only when the given expression
evaluates into true.

The given expression is assumed to return either NULL (treated as false) or
a boolean value.")
  (:public
   #>cpp
   Filter() {}

   Filter(const std::shared_ptr<LogicalOperator> &input_,
          Expression *expression_);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class FilterCursor : public Cursor {
    public:
     FilterCursor(const Filter &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const Filter &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class produce (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (named-expressions "std::vector<NamedExpression *>" :scope :public
                      :slk-save #'slk-save-ast-vector
                      :slk-load (slk-load-ast-vector "NamedExpression")))
  (:documentation
   "A logical operator that places an arbitrary number
of named expressions on the frame (the logical operator
for the RETURN clause).

Supports optional input. When the input is provided,
it is Pulled from and the Produce succeeds once for
every input Pull (typically a MATCH/RETURN query).
When the input is not provided (typically a standalone
RETURN clause) the Produce's pull succeeds exactly once.")
  (:public
   #>cpp
   Produce() {}

   Produce(const std::shared_ptr<LogicalOperator> &input,
           const std::vector<NamedExpression *> &named_expressions);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class ProduceCursor : public Cursor {
    public:
     ProduceCursor(const Produce &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const Produce &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class delete (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (expressions "std::vector<Expression *>" :scope :public
                :slk-save #'slk-save-ast-vector
                :slk-load (slk-load-ast-vector "Expression"))
   (detach :bool :scope :public :documentation
           "Whether the vertex should be detached before deletion. If not detached,
           and has connections, an error is raised when deleting edges."))
  (:documentation
   "Operator for deleting vertices and edges.

Has a flag for using DETACH DELETE when deleting vertices.")
  (:public
   #>cpp
   Delete() {}

   Delete(const std::shared_ptr<LogicalOperator> &input_,
          const std::vector<Expression *> &expressions, bool detach_);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class DeleteCursor : public Cursor {
    public:
     DeleteCursor(const Delete &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const Delete &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class set-property (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (property "::storage::v3::PropertyId" :scope :public)
   (lhs "PropertyLookup *" :scope :public
        :slk-save #'slk-save-ast-pointer
        :slk-load (slk-load-ast-pointer "PropertyLookup"))
   (rhs "Expression *" :scope :public
        :slk-save #'slk-save-ast-pointer
        :slk-load (slk-load-ast-pointer "Expression")))
  (:documentation
   "Logical operator for setting a single property on a single vertex or edge.

The property value is an expression that must evaluate to some type that
can be stored (a TypedValue that can be converted to PropertyValue).")
  (:public
   #>cpp
   SetProperty() {}

   SetProperty(const std::shared_ptr<LogicalOperator> &input,
               storage::v3::PropertyId property, PropertyLookup *lhs,
               Expression *rhs);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class SetPropertyCursor : public Cursor {
    public:
     SetPropertyCursor(const SetProperty &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const SetProperty &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class set-properties (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (input-symbol "Symbol" :scope :public)
   (rhs "Expression *" :scope :public
        :slk-save #'slk-save-ast-pointer
        :slk-load (slk-load-ast-pointer "Expression"))
   (op "Op" :scope :public))
  (:documentation
   "Logical operator for setting the whole property set on a vertex or an edge.

The value being set is an expression that must evaluate to a vertex, edge or
map (literal or parameter).

Supports setting (replacing the whole properties set with another) and
updating.")
  (:public
   (lcp:define-enum op
     (update replace)
     (:documentation "Defines how setting the properties works.

@c UPDATE means that the current property set is augmented with additional
ones (existing props of the same name are replaced), while @c REPLACE means
that the old properties are discarded and replaced with new ones.")
     (:serialize))

   #>cpp
   SetProperties() {}

   SetProperties(const std::shared_ptr<LogicalOperator> &input,
                 Symbol input_symbol, Expression *rhs, Op op);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class SetPropertiesCursor : public Cursor {
    public:
     SetPropertiesCursor(const SetProperties &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const SetProperties &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class set-labels (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (input-symbol "Symbol" :scope :public)
   (labels "std::vector<storage::v3::LabelId>" :scope :public))
  (:documentation
   "Logical operator for setting an arbitrary number of labels on a Vertex.

It does NOT remove labels that are already set on that Vertex.")
  (:public
   #>cpp
   SetLabels() {}

   SetLabels(const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
             const std::vector<storage::v3::LabelId> &labels);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class SetLabelsCursor : public Cursor {
    public:
     SetLabelsCursor(const SetLabels &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const SetLabels &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class remove-property (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (property "::storage::v3::PropertyId" :scope :public)
   (lhs "PropertyLookup *" :scope :public
        :slk-save #'slk-save-ast-pointer
        :slk-load (slk-load-ast-pointer "PropertyLookup")))
  (:documentation
   "Logical operator for removing a property from an edge or a vertex.")
  (:public
   #>cpp
   RemoveProperty() {}

   RemoveProperty(const std::shared_ptr<LogicalOperator> &input,
                  storage::v3::PropertyId property, PropertyLookup *lhs);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class RemovePropertyCursor : public Cursor {
    public:
     RemovePropertyCursor(const RemoveProperty &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const RemoveProperty &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class remove-labels (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (input-symbol "Symbol" :scope :public)
   (labels "std::vector<storage::v3::LabelId>" :scope :public))
  (:documentation
   "Logical operator for removing an arbitrary number of labels on a Vertex.

If a label does not exist on a Vertex, nothing happens.")
  (:public
   #>cpp
   RemoveLabels() {}

   RemoveLabels(const std::shared_ptr<LogicalOperator> &input,
                Symbol input_symbol, const std::vector<storage::v3::LabelId> &labels);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class RemoveLabelsCursor : public Cursor {
    public:
     RemoveLabelsCursor(const RemoveLabels &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const RemoveLabels &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class edge-uniqueness-filter (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (expand-symbol "Symbol" :scope :public)
   (previous-symbols "std::vector<Symbol>" :scope :public))
  (:documentation
   "Filter whose Pull returns true only when the given expand_symbol frame
value (the latest expansion) is not equal to any of the previous_symbols frame
values.

Used for implementing Cyphermorphism.
Isomorphism is vertex-uniqueness. It means that two different vertices in a
pattern can not map to the same data vertex.
Cyphermorphism is edge-uniqueness (the above explanation applies). By default
Neo4j uses Cyphermorphism (that's where the name stems from, it is not a valid
graph-theory term).

Supports variable-length-edges (uniqueness comparisons between edges and an
edge lists).")
  (:public
   #>cpp
   EdgeUniquenessFilter() {}

   EdgeUniquenessFilter(const std::shared_ptr<LogicalOperator> &input,
                        Symbol expand_symbol,
                        const std::vector<Symbol> &previous_symbols);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class EdgeUniquenessFilterCursor : public Cursor {
    public:
     EdgeUniquenessFilterCursor(const EdgeUniquenessFilter &,
                                utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const EdgeUniquenessFilter &self_;
     const UniqueCursorPtr input_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class accumulate (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (symbols "std::vector<Symbol>" :scope :public)
   (advance-command :bool :scope :public))
  (:documentation
   "Pulls everything from the input before passing it through.
Optionally advances the command after accumulation and before emitting.

On the first Pull from this operator's Cursor the input Cursor will be Pulled
until it is empty. The results will be accumulated in the temporary cache. Once
the input Cursor is empty, this operator's Cursor will start returning cached
stuff from its Pull.

This technique is used for ensuring all the operations from the
previous logical operator have been performed before exposing data
to the next. A typical use case is a `MATCH--SET--RETURN`
query in which every SET iteration must be performed before
RETURN starts iterating (see Memgraph Wiki for detailed reasoning).

IMPORTANT: This operator does not cache all the results but only those
elements from the frame whose symbols (frame positions) it was given.
All other frame positions will contain undefined junk after this
operator has executed, and should not be used.

This operator can also advance the command after the accumulation and
before emitting. If the command gets advanced, every value that
has been cached will be reconstructed before Pull returns.

@param input Input @c LogicalOperator.
@param symbols A vector of Symbols that need to be accumulated
 and exposed to the next op.")
  (:public
   #>cpp
   Accumulate() {}

   Accumulate(const std::shared_ptr<LogicalOperator> &input,
              const std::vector<Symbol> &symbols, bool advance_command = false);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class aggregate (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (aggregations "std::vector<Element>" :scope :public
                 :slk-save (lambda (member)
                             #>cpp
                             size_t size = self.${member}.size();
                             slk::Save(size, builder);
                             for (const auto &v : self.${member}) {
                               slk::Save(v, builder, helper);
                             }
                             cpp<#)
                 :slk-load (lambda (member)
                             #>cpp
                             size_t size;
                             slk::Load(&size, reader);
                             self->${member}.resize(size);
                             for (size_t i = 0;
                                  i < size;
                                  ++i) {
                               slk::Load(&self->${member}[i], reader, helper);
                             }
                             cpp<#))
   (group-by "std::vector<Expression *>" :scope :public
             :slk-save #'slk-save-ast-vector
             :slk-load (slk-load-ast-vector "Expression"))
   (remember "std::vector<Symbol>" :scope :public))
  (:documentation
   "Performs an arbitrary number of aggregations of data
from the given input grouped by the given criteria.

Aggregations are defined by triples that define
(input data expression, type of aggregation, output symbol).
Input data is grouped based on the given set of named
expressions. Grouping is done on unique values.

IMPORTANT:
Operators taking their input from an aggregation are only
allowed to use frame values that are either aggregation
outputs or group-by named-expressions. All other frame
elements are in an undefined state after aggregation.")
  (:public
   (lcp:define-struct element ()
     ((value "Expression *"
             :slk-save #'slk-save-ast-pointer
             :slk-load (slk-load-ast-pointer "Expression"))
      (key "Expression *"
           :slk-save #'slk-save-ast-pointer
           :slk-load (slk-load-ast-pointer "Expression"))
      (op "::Aggregation::Op")
      (output-sym "Symbol"))
     (:documentation
      "An aggregation element, contains:
       (input data expression, key expression - only used in COLLECT_MAP, type of
       aggregation, output symbol).")
     (:serialize (:slk :save-args '((helper "query::v2::plan::LogicalOperator::SaveHelper *"))
                       :load-args '((helper "query::v2::plan::LogicalOperator::SlkLoadHelper *"))))
     (:clone :args '((storage "AstStorage *"))))
   #>cpp
   Aggregate() = default;
   Aggregate(const std::shared_ptr<LogicalOperator> &input,
             const std::vector<Element> &aggregations,
             const std::vector<Expression *> &group_by,
             const std::vector<Symbol> &remember);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class skip (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (expression "Expression *" :scope :public
               :slk-save #'slk-save-ast-pointer
               :slk-load (slk-load-ast-pointer "Expression")))
  (:documentation
   "Skips a number of Pulls from the input op.

The given expression determines how many Pulls from the input
should be skipped (ignored).
All other successful Pulls from the
input are simply passed through.

The given expression is evaluated after the first Pull from
the input, and only once. Neo does not allow this expression
to contain identifiers, and neither does Memgraph, but this
operator's implementation does not expect this.")
  (:public
   #>cpp
   Skip() {}

   Skip(const std::shared_ptr<LogicalOperator> &input, Expression *expression);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class SkipCursor : public Cursor {
    public:
     SkipCursor(const Skip &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const Skip &self_;
     const UniqueCursorPtr input_cursor_;
     // init to_skip_ to -1, indicating
     // that it's still unknown (input has not been Pulled yet)
     int64_t to_skip_{-1};
     int64_t skipped_{0};
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class limit (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (expression "Expression *" :scope :public
               :slk-save #'slk-save-ast-pointer
               :slk-load (slk-load-ast-pointer "Expression")))
  (:documentation
   "Limits the number of Pulls from the input op.

The given expression determines how many
input Pulls should be passed through. The input is not
Pulled once this limit is reached. Note that this has
implications: the out-of-bounds input Pulls are never
evaluated.

The limit expression must NOT use anything from the
Frame. It is evaluated before the first Pull from the
input. This is consistent with Neo (they don't allow
identifiers in limit expressions), and it's necessary
when limit evaluates to 0 (because 0 Pulls from the
input should be performed).")
  (:public
   #>cpp
   Limit() {}

   Limit(const std::shared_ptr<LogicalOperator> &input, Expression *expression);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class LimitCursor : public Cursor {
    public:
     LimitCursor(const Limit &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const Limit &self_;
     UniqueCursorPtr input_cursor_;
     // init limit_ to -1, indicating
     // that it's still unknown (Cursor has not been Pulled yet)
     int64_t limit_{-1};
     int64_t pulled_{0};
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class order-by (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (compare "TypedValueVectorCompare" :scope :public)
   (order-by "std::vector<Expression *>" :scope :public
             :slk-save #'slk-save-ast-vector
             :slk-load (slk-load-ast-vector "Expression"))
   (output-symbols "std::vector<Symbol>" :scope :public))
  (:documentation
   "Logical operator for ordering (sorting) results.

Sorts the input rows based on an arbitrary number of
Expressions. Ascending or descending ordering can be chosen
for each independently (not providing enough orderings
results in a runtime error).

For each row an arbitrary number of Frame elements can be
remembered. Only these elements (defined by their Symbols)
are valid for usage after the OrderBy operator.")
  (:public
   #>cpp
   OrderBy() {}

   OrderBy(const std::shared_ptr<LogicalOperator> &input,
           const std::vector<SortItem> &order_by,
           const std::vector<Symbol> &output_symbols);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class merge (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (merge-match "std::shared_ptr<LogicalOperator>" :scope :public
                :slk-save #'slk-save-operator-pointer
                :slk-load #'slk-load-operator-pointer)
   (merge-create "std::shared_ptr<LogicalOperator>" :scope :public
                 :slk-save #'slk-save-operator-pointer
                 :slk-load #'slk-load-operator-pointer))
  (:documentation
   "Merge operator. For every sucessful Pull from the
input operator a Pull from the merge_match is attempted. All
successfull Pulls from the merge_match are passed on as output.
If merge_match Pull does not yield any elements, a single Pull
from the merge_create op is performed.

The input logical op is optional. If false (nullptr)
it will be replaced by a Once op.

For an argumentation of this implementation see the wiki
documentation.")
  (:public
   #>cpp
   Merge() {}

   Merge(const std::shared_ptr<LogicalOperator> &input,
         const std::shared_ptr<LogicalOperator> &merge_match,
         const std::shared_ptr<LogicalOperator> &merge_create);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   // TODO: Consider whether we want to treat Merge as having single input. It
   // makes sense that we do, because other branches are executed depending on
   // the input.
   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class MergeCursor : public Cursor {
    public:
     MergeCursor(const Merge &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const UniqueCursorPtr input_cursor_;
     const UniqueCursorPtr merge_match_cursor_;
     const UniqueCursorPtr merge_create_cursor_;

     // indicates if the next Pull from this cursor
     // should perform a pull from input_cursor_
     // this is true when:
     //  - first Pulling from this cursor
     //  - previous Pull from this cursor exhausted the merge_match_cursor
     bool pull_input_{true};
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class optional (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (optional "std::shared_ptr<LogicalOperator>" :scope :public
             :slk-save #'slk-save-operator-pointer
             :slk-load #'slk-load-operator-pointer)
   (optional-symbols "std::vector<Symbol>" :scope :public))
  (:documentation
   "Optional operator. Used for optional match. For every
successful Pull from the input branch a Pull from the optional
branch is attempted (and Pulled from till exhausted). If zero
Pulls succeed from the optional branch, the Optional operator
sets the optional symbols to TypedValue::Null on the Frame
and returns true, once.")
  (:public
   #>cpp
   Optional() {}

   Optional(const std::shared_ptr<LogicalOperator> &input,
            const std::shared_ptr<LogicalOperator> &optional,
            const std::vector<Symbol> &optional_symbols);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:private
   #>cpp
   class OptionalCursor : public Cursor {
    public:
     OptionalCursor(const Optional &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const Optional &self_;
     const UniqueCursorPtr input_cursor_;
     const UniqueCursorPtr optional_cursor_;
     // indicates if the next Pull from this cursor should
     // perform a Pull from the input_cursor_
     // this is true when:
     //  - first pulling from this Cursor
     //  - previous Pull from this cursor exhausted the optional_cursor_
     bool pull_input_{true};
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class unwind (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (input-expression "Expression *" :scope :public
                     :slk-save #'slk-save-ast-pointer
                     :slk-load (slk-load-ast-pointer "Expression"))
   (output-symbol "Symbol" :scope :public))
  (:documentation
   "Takes a list TypedValue as it's input and yields each
element as it's output.

Input is optional (unwind can be the first clause in a query).")
  (:public
   #>cpp
   Unwind() {}

   Unwind(const std::shared_ptr<LogicalOperator> &input,
          Expression *input_expression_, Symbol output_symbol);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override {
  return true; }
   std::shared_ptr<LogicalOperator> input() const override {
  return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class distinct (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (value-symbols "std::vector<Symbol>" :scope :public))
  (:documentation
   "Ensures that only distinct rows are yielded.
This implementation accepts a vector of Symbols
which define a row. Only those Symbols are valid
for use in operators following Distinct.

This implementation maintains input ordering.")
  (:public
   #>cpp
   Distinct() {}

   Distinct(const std::shared_ptr<LogicalOperator> &input,
            const std::vector<Symbol> &value_symbols);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = input;
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class union (logical-operator)
  ((left-op "std::shared_ptr<LogicalOperator>" :scope :public
            :slk-save #'slk-save-operator-pointer
            :slk-load #'slk-load-operator-pointer)
   (right-op "std::shared_ptr<LogicalOperator>" :scope :public
             :slk-save #'slk-save-operator-pointer
             :slk-load #'slk-load-operator-pointer)
   (union-symbols "std::vector<Symbol>" :scope :public)
   (left-symbols "std::vector<Symbol>" :scope :public)
   (right-symbols "std::vector<Symbol>" :scope :public))
  (:documentation
   "A logical operator that applies UNION operator on inputs and places the
result on the frame.

This operator takes two inputs, a vector of symbols for the result, and vectors
of symbols used by each of the inputs.")
  (:public
   #>cpp
   Union() {}

   Union(const std::shared_ptr<LogicalOperator> &left_op,
         const std::shared_ptr<LogicalOperator> &right_op,
         const std::vector<Symbol> &union_symbols,
         const std::vector<Symbol> &left_symbols,
         const std::vector<Symbol> &right_symbols);
   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

   bool HasSingleInput() const override;
   std::shared_ptr<LogicalOperator> input() const override;
   void set_input(std::shared_ptr<LogicalOperator>) override;
   cpp<#)
  (:private
   #>cpp
   class UnionCursor : public Cursor {
    public:
     UnionCursor(const Union &, utils::MemoryResource *);
     bool Pull(Frames &, ExecutionContext &) override;
     void Shutdown() override;
     void Reset() override;

    private:
     const Union &self_;
     const UniqueCursorPtr left_cursor_, right_cursor_;
   };
   cpp<#)
  (:serialize (:slk))
  (:clone))

;; TODO: We should probably output this operator in regular planner, not just
;; distributed planner.
(lcp:define-class cartesian (logical-operator)
  ((left-op "std::shared_ptr<LogicalOperator>" :scope :public
            :slk-save #'slk-save-operator-pointer
            :slk-load #'slk-load-operator-pointer)
   (left-symbols "std::vector<Symbol>" :scope :public)
   (right-op "std::shared_ptr<LogicalOperator>" :scope :public
             :slk-save #'slk-save-operator-pointer
             :slk-load #'slk-load-operator-pointer)
   (right-symbols "std::vector<Symbol>" :scope :public))
  (:documentation
   "Operator for producing a Cartesian product from 2 input branches")
  (:public
    #>cpp
    Cartesian() {}
    /** Construct the operator with left input branch and right input branch. */
    Cartesian(const std::shared_ptr<LogicalOperator> &left_op,
              const std::vector<Symbol> &left_symbols,
              const std::shared_ptr<LogicalOperator> &right_op,
              const std::vector<Symbol> &right_symbols)
        : left_op_(left_op),
          left_symbols_(left_symbols),
          right_op_(right_op),
          right_symbols_(right_symbols) {}

    bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
    std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

    bool HasSingleInput() const override;
    std::shared_ptr<LogicalOperator> input() const override;
    void set_input(std::shared_ptr<LogicalOperator>) override;
    cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class output-table (logical-operator)
  ((output-symbols "std::vector<Symbol>" :scope :public :dont-save t)
   (callback "std::function<std::vector<std::vector<TypedValue>>(Frames &, ExecutionContext *)>"
             :scope :public :dont-save t :clone :copy))
  (:documentation "An operator that outputs a table, producing a single row on each pull")
  (:public
   #>cpp
   OutputTable() {}
   OutputTable(
       std::vector<Symbol> output_symbols,
       std::function<std::vector<std::vector<TypedValue>>(Frames &, ExecutionContext *)>
           callback);
   OutputTable(std::vector<Symbol> output_symbols,
               std::vector<std::vector<TypedValue>> rows);

   bool Accept(HierarchicalLogicalOperatorVisitor &) override {
     LOG_FATAL("OutputTable operator should not be visited!");
   }

   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override {
     return output_symbols_;
   }
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override {
     return output_symbols_;
   }

   bool HasSingleInput() const override;
   std::shared_ptr<LogicalOperator> input() const override;
   void set_input(std::shared_ptr<LogicalOperator> input) override;
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class output-table-stream (logical-operator)
  ((output-symbols "std::vector<Symbol>" :scope :public :dont-save t)
   (callback "std::function<std::optional<std::vector<TypedValue>>(Frames &, ExecutionContext *)>"
             :scope :public :dont-save t :clone :copy))
  (:documentation "An operator that outputs a table, producing a single row on each pull.
This class is different from @c OutputTable in that its callback doesn't fetch all rows
at once. Instead, each call of the callback should return a single row of the table.")
  (:public
   #>cpp
   OutputTableStream() {}
   OutputTableStream(
       std::vector<Symbol> output_symbols,
       std::function<std::optional<std::vector<TypedValue>>(Frames &, ExecutionContext *)>
           callback);

   bool Accept(HierarchicalLogicalOperatorVisitor &) override {
     LOG_FATAL("OutputTableStream operator should not be visited!");
   }

   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> OutputSymbols(const SymbolTable &) const override {
     return output_symbols_;
   }
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override {
     return output_symbols_;
   }

   bool HasSingleInput() const override;
   std::shared_ptr<LogicalOperator> input() const override;
   void set_input(std::shared_ptr<LogicalOperator> input) override;
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class call-procedure (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (procedure-name "std::string" :scope :public)
   (arguments "std::vector<Expression *>"
              :scope :public
              :slk-save #'slk-save-ast-vector
              :slk-load (slk-load-ast-vector "Expression"))
   (result-fields "std::vector<std::string>" :scope :public)
   (result-symbols "std::vector<Symbol>" :scope :public)
   (memory-limit "Expression *" :initval "nullptr" :scope :public
                 :slk-save #'slk-save-ast-pointer
                 :slk-load (slk-load-ast-pointer "Expression"))
   (memory-scale "size_t" :initval "1024U" :scope :public)
   (is_write :bool  :scope :public))
  (:public
    #>cpp
    CallProcedure() = default;
    CallProcedure(std::shared_ptr<LogicalOperator> input, std::string name,
                  std::vector<Expression *> arguments,
                  std::vector<std::string> fields, std::vector<Symbol> symbols,
                  Expression *memory_limit, size_t memory_scale, bool is_write);

    bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
    UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
    std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
    std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

    bool HasSingleInput() const override { return true; }
    std::shared_ptr<LogicalOperator> input() const override { return input_; }
    void set_input(std::shared_ptr<LogicalOperator> input) override {
      input_ = input;
    }

    static void IncrementCounter(const std::string &procedure_name);
    static std::unordered_map<std::string, int64_t> GetAndResetCounters();
    cpp<#)
  (:private
    #>cpp
    inline static utils::Synchronized<std::unordered_map<std::string, int64_t>, utils::SpinLock> procedure_counters_;
    cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class load-csv (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (file "Expression *" :scope :public)
   (with_header "bool" :scope :public)
   (ignore_bad "bool" :scope :public)
   (delimiter "Expression *" :initval "nullptr" :scope :public
                 :slk-save #'slk-save-ast-pointer
                 :slk-load (slk-load-ast-pointer "Expression"))
   (quote "Expression *" :initval "nullptr" :scope :public
                 :slk-save #'slk-save-ast-pointer
                 :slk-load (slk-load-ast-pointer "Expression"))
   (row_var "Symbol" :scope :public))
  (:public
    #>cpp
    LoadCsv() = default;
    LoadCsv(std::shared_ptr<LogicalOperator> input, Expression *file, bool with_header, bool ignore_bad,
            Expression* delimiter, Expression* quote, Symbol row_var);
    bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
    UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
    std::vector<Symbol> OutputSymbols(const SymbolTable &) const override;
    std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;

    bool HasSingleInput() const override { return true; }
    std::shared_ptr<LogicalOperator> input() const override { return input_; }
    void set_input(std::shared_ptr<LogicalOperator> input) override {
      input_ = input;
    }
    cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:define-class foreach (logical-operator)
  ((input "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (update-clauses "std::shared_ptr<LogicalOperator>" :scope :public
          :slk-save #'slk-save-operator-pointer
          :slk-load #'slk-load-operator-pointer)
   (expression "Expression *" :scope :public
           :slk-save #'slk-save-ast-pointer
           :slk-load (slk-load-ast-pointer "Expression"))
   (loop-variable-symbol "Symbol" :scope :public))

  (:documentation
   "Iterates over a collection of elements and applies one or more update
clauses.
")
  (:public
   #>cpp
   Foreach() = default;
   Foreach(std::shared_ptr<LogicalOperator> input,
           std::shared_ptr<LogicalOperator> updates,
           Expression *named_expr,
           Symbol loop_variable_symbol);

   bool Accept(HierarchicalLogicalOperatorVisitor &visitor) override;
   UniqueCursorPtr MakeCursor(utils::MemoryResource *) const override;
   std::vector<Symbol> ModifiedSymbols(const SymbolTable &) const override;
   bool HasSingleInput() const override { return true; }
   std::shared_ptr<LogicalOperator> input() const override { return input_; }
   void set_input(std::shared_ptr<LogicalOperator> input) override {
     input_ = std::move(input);
   }
   cpp<#)
  (:serialize (:slk))
  (:clone))

(lcp:pop-namespace) ;; distributed
(lcp:pop-namespace) ;; plan
(lcp:pop-namespace) ;; v2
(lcp:pop-namespace) ;; query
(lcp:pop-namespace) ;; memgraph