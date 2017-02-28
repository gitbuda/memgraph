#pragma once

#include "database/db_accessor.hpp"
#include "storage/model/property_value.hpp"
#include "storage/model/property_value_store.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/assert.hpp"

// Holder for element data which he can then insert as a vertex or edge into the
// database depending on the available data and called add_* method.
class ElementSkeleton {
 public:
  ElementSkeleton(DbAccessor &db) : db(db){};

  void add_property(StoredProperty<TypeGroupVertex> &&prop) {
    properties_v.push_back(std::move(prop));
  }

  void add_property(StoredProperty<TypeGroupEdge> &&prop) {
    properties_e.push_back(std::move(prop));
  }

  void set_element_id(size_t id) { el_id = make_option<size_t>(std::move(id)); }

  void add_label(Label const &label) { labels.push_back(&label); }

  void set_type(EdgeType const &type) { this->type = make_option(&type); }

  void set_from(VertexAccessor &&va) {
    from_va = make_option<VertexAccessor>(std::move(va));
  }

  void set_to(VertexAccessor &&va) {
    to_va = make_option<VertexAccessor>(std::move(va));
  }

  VertexAccessor add_vertex() {
    debug_assert(properties_e.empty(), "Properties aren't empty.");

    auto va = db.vertex_insert();

    for (auto l : labels) {
      // std::cout << *l << std::endl;
      va.add_label(*l);
    }

    for (auto prop : properties_v) {
      va.set(std::move(prop));
    }

    return va;
  }

  // Return error msg if unsuccessful
  Option<std::string> add_edge() {
    if (!from_va.is_present()) {
      return make_option(std::string("From field must be set"));
    }
    if (!to_va.is_present()) {
      return make_option(std::string("To field must be set"));
    }
    if (!type.is_present()) {
      return make_option(std::string("Type field must be set"));
    }
    debug_assert(properties_v.empty(), "Properties aren't empty.");

    auto ve = db.edge_insert(from_va.get(), to_va.get());
    ve.edge_type(*type.get());

    for (auto prop : properties_e) {
      ve.set(std::move(prop));
    }

    return make_option<std::string>();
  }

  void clear() {
    el_id = make_option<size_t>();
    to_va = make_option<VertexAccessor>();
    from_va = make_option<VertexAccessor>();
    type = make_option<EdgeType const *>();
    labels.clear();
    properties_v.clear();
    properties_e.clear();
  }

  // Returns import local id.
  Option<size_t> element_id() { return el_id; }

 private:
  DbAccessor &db;

  Option<size_t> el_id;
  Option<VertexAccessor> to_va;
  Option<VertexAccessor> from_va;
  Option<EdgeType const *> type;
  std::vector<Label const *> labels;
  PropertyValueStore properties_e;
  PropertyValueStore properties_v;
};
