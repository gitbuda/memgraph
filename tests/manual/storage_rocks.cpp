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

#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"

#include "storage/rocks/storage.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"

void PutKey(rocksdb::DB *db, const rocksdb::WriteOptions &opt, const std::string &key, const std::string &value) {
  auto s = db->Put(opt, key, value);
  MG_ASSERT(s.ok());
}

void NoKey(rocksdb::DB *db, const rocksdb::ReadOptions &opt, const std::string &key) {
  std::string value;
  auto s = db->Get(opt, key, &value);
  MG_ASSERT(s.IsNotFound());
  SPDLOG_INFO("no {} key", key);
}

void YesKey(rocksdb::DB *db, const rocksdb::ReadOptions &opt, const std::string &key) {
  std::string value;
  auto s = db->Get(opt, key, &value);
  MG_ASSERT(s.ok());
  SPDLOG_INFO("{}: {}", key, value);
}

void PutKey(rocksdb::Transaction *txn, const std::string &key, const std::string &value) {
  auto s = txn->Put(key, value);
  MG_ASSERT(s.ok());
}

void NoKey(rocksdb::Transaction *txn, const rocksdb::ReadOptions &opt, const std::string &key) {
  std::string value;
  auto s = txn->Get(opt, key, &value);
  MG_ASSERT(s.IsNotFound());
  SPDLOG_INFO("no {} key", key);
}

void YesKey(rocksdb::Transaction *txn, const rocksdb::ReadOptions &opt, const std::string &key) {
  std::string value;
  auto s = txn->Get(opt, key, &value);
  MG_ASSERT(s.ok());
  SPDLOG_INFO("{}: {}", key, value);
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::info);

  std::filesystem::path storage = "rocks_experiment";
  if (!memgraph::utils::EnsureDir(storage)) {
    SPDLOG_ERROR("Unable to create storage folder on disk.");
    return 1;
  }
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  rocksdb::OptimisticTransactionOptions txn_options;
  txn_options.set_snapshot = true;
  rocksdb::OptimisticTransactionDB *txn_db;
  auto s = rocksdb::OptimisticTransactionDB::Open(options, storage, &txn_db);
  MG_ASSERT(s.ok());
  db = txn_db->GetBaseDB();

  PutKey(db, rocksdb::WriteOptions(), "key1", "value1");
  YesKey(db, rocksdb::ReadOptions(), "key1");

  rocksdb::WriteOptions write_options;
  auto *txn = txn_db->BeginTransaction(write_options, txn_options);

  rocksdb::ReadOptions read_options;
  // The transaction won't commit if someone outside changed the same value
  // this transaction intends to change. In other words, guarantee that noone
  // else has written a key since the start of the transaction.
  // txn->SetSnapshot();
  read_options.snapshot = txn->GetSnapshot();

  PutKey(txn, "key2", "value2");
  YesKey(txn, read_options, "key2");

  s = txn->Commit();
  assert(s.ok());
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;

  YesKey(db, rocksdb::ReadOptions(), "key1");
  YesKey(db, rocksdb::ReadOptions(), "key2");

  delete txn_db;

  memgraph::storage::rocks::Storage mgrocks;
  mgrocks.Access();

  return 0;
}
