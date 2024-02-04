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

#include <iostream>
#include <memory>

#include "flags/general.hpp"
#include "flags/log_level.hpp"
#include "storage/v2/inmemory/storage.hpp"

int main() {
  memgraph::storage::Config config;
  std::unique_ptr<memgraph::storage::Storage> store = std::make_unique<memgraph::storage::InMemoryStorage>(config);

  // TODO(gitbuda): Inject schema/Gid
  // TODO(gitbuda): Inject transaction

  auto acc = store->Access();
  auto property_key_id = acc->NameToProperty("property");
  auto property_value = memgraph::storage::PropertyValue("value");
  auto v_acc = acc->CreateVertex();
  auto prop_set_res = v_acc.SetProperty(property_key_id, property_value);
  if (prop_set_res.HasError()) {
    std::exit(1);
  }

  if (auto vertex = acc->FindVertex(v_acc.Gid(), memgraph::storage::View::NEW); vertex) {
    if (auto property_value = vertex->GetProperty(property_key_id, memgraph::storage::View::NEW);
        property_value.HasValue()) {
      std::cout << property_value->ValueString() << std::endl;
    } else {
      std::exit(1);
    }
  }

  auto commit_res = acc->Commit();
  if (commit_res.HasError()) {
    std::exit(1);
  }

  return 0;
}
