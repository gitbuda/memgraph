#include <vector>

#include <gtest/gtest.h>

#include "storage/common/mvcc/version.hpp"
#include "storage/single_node/mvcc/record.hpp"
#include "storage/single_node/mvcc/version_list.hpp"
#include "transactions/single_node/engine.hpp"
#include "transactions/transaction.hpp"

class TestClass : public mvcc::Record<TestClass> {
 public:
  // constructs first version, size should be 0
  explicit TestClass(int &version_list_size) : version_list_size_(version_list_size) {
    ++version_list_size_;
  }
  TestClass *CloneData() { return new TestClass(version_list_size_); }
  // version constructed in version list update
  TestClass(TestClass &other) : version_list_size_(other.version_list_size_) {
    version_list_size_++;
  }
  friend std::ostream &operator<<(std::ostream &stream, TestClass &test_class) {
    stream << test_class.tx().cre << " " << test_class.tx().exp;
    return stream;
  }
  // reference to variable version_list_size in test SetUp, increases when new
  // TestClass is created
  int &version_list_size_;
};

/**
 * Testing mvcc::VersionList::find behavior in
 * different situations (preceeding update/remove ops
 * in different transactions).
 *
 * The setup for each case is:
 *  - transaction t1 has created a new version_list v1 and commited
 *  - transaction t2 has strated
 *  - *********************
 *  - here the test fixture ends and custom test behavior should be added
 *  - *********************
 *  - tests should check every legal sequence of the following ops
 *      - creation of transaction t3
 *      - [commit/abort] of [t2/t3]
 *      - [removal/update] on version_list by [t2/t3]
 *  - illegal sequences (for example double commit) don't have to be checked
 */
class Mvcc : public ::testing::Test {
 protected:
  virtual void SetUp() {
    id0 = 0;
    engine.Advance(t1->id_);
    id1 = t1->id_;
    v1 = version_list.find(*t1);
    engine.Commit(*t1);
    t2 = engine.Begin();
    id2 = t2->id_;
  }
  // variable where number of versions is stored
  int version_list_size = 0;
  tx::Engine engine;
  tx::Transaction *t1 = engine.Begin();
  mvcc::VersionList<TestClass> version_list{*t1, storage::Gid::FromInt(0),
                                            version_list_size};
  TestClass *v1 = nullptr;
  tx::Transaction *t2 = nullptr;
  tx::TransactionId id0, id1, id2;
};

// helper macros. important:
//  - TX_FIND and TX_UPDATE set the record variable vX
//  - TX_BEGIN sets the transaction variable tX
#define T2_FIND __attribute__((unused)) auto v2 = version_list.find(*t2)
#define T3_FIND __attribute__((unused)) auto v3 = version_list.find(*t3)
#define T4_FIND __attribute__((unused)) auto v4 = version_list.find(*t4)
#define T2_UPDATE __attribute__((unused)) auto v2 = version_list.update(*t2)
#define T3_UPDATE __attribute__((unused)) auto v3 = version_list.update(*t3)
#define T2_COMMIT engine.Commit(*t2);
#define T3_COMMIT engine.Commit(*t3);
#define T2_ABORT engine.Abort(*t2);
#define T3_ABORT engine.Abort(*t3);
#define T3_BEGIN            \
  auto t3 = engine.Begin(); \
  __attribute__((unused)) int id3 = t3->id_
#define T4_BEGIN auto t4 = engine.Begin();
#define T2_REMOVE version_list.remove(version_list.find(*t2), *t2)
#define T3_REMOVE version_list.remove(version_list.find(*t3), *t3)
#define EXPECT_CRE(record, expected) EXPECT_EQ(record->tx().cre, id##expected)
#define EXPECT_EXP(record, expected) EXPECT_EQ(record->tx().exp, id##expected)
#define EXPECT_NXT(v1, v2) EXPECT_EQ(v1->next(), v2)
#define EXPECT_SIZE(n) EXPECT_EQ(version_list_size, n)

// test the fixture
TEST_F(Mvcc, Fixture) {
  EXPECT_CRE(v1, 1);
  EXPECT_EXP(v1, 0);
}
