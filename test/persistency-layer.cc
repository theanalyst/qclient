/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                           *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "gtest/gtest.h"
#include "RocksDBPersistencyFixture.hh"
#include "qclient/BackgroundFlusher.hh"
#include "qclient/MemoryPersistency.hh"
#include "qclient/RocksDBPersistency.hh"


using namespace qclient;

using q_item_t = std::vector<std::string>;

TEST(PersistencyLayer, DummyPersistency)
{
   auto dummy_persistency = std::make_unique<PersistencyLayer<q_item_t>>();
   ASSERT_EQ(0, dummy_persistency->getStartingIndex());
   ASSERT_EQ(0, dummy_persistency->getEndingIndex());
   q_item_t out;
   q_item_t in{"hello", "world"};
   dummy_persistency->record(42, in);
   ASSERT_FALSE(dummy_persistency->retrieve(42,out));
}


TEST(PersistencyLayer, StubInMemoryPersistency)
{
   auto in_memory_persistency = std::make_unique<StubInMemoryPersistency<q_item_t>>();
   ASSERT_EQ(0, in_memory_persistency->getStartingIndex());
   ASSERT_EQ(0, in_memory_persistency->getEndingIndex());
   q_item_t out;
   q_item_t in{"hello", "world"};
   in_memory_persistency->record(42, in);
   ASSERT_TRUE(in_memory_persistency->retrieve(42,out));
   ASSERT_EQ(in_memory_persistency->getEndingIndex(), 1);
   ASSERT_EQ(in_memory_persistency->getStartingIndex(), 0);
   ASSERT_EQ(in, out);
   in_memory_persistency->pop();
   ASSERT_FALSE(in_memory_persistency->retrieve(42,out));
}

TEST(PersistencyLayer, InMemoryPeristencyPop)
{
  auto in_memory_persistency = std::make_unique<StubInMemoryPersistency<q_item_t>>();
  ASSERT_EQ(0, in_memory_persistency->getStartingIndex());
  ASSERT_EQ(0, in_memory_persistency->getEndingIndex());
  in_memory_persistency->record(0, {"hello", "world"});
  in_memory_persistency->record(1, {"foo", "bar"});

  ASSERT_EQ(0, in_memory_persistency->getStartingIndex());
  ASSERT_EQ(2, in_memory_persistency->getEndingIndex());
  in_memory_persistency->pop();
  ASSERT_EQ(1, in_memory_persistency->getStartingIndex());
  ASSERT_EQ(2, in_memory_persistency->getEndingIndex());
  in_memory_persistency->pop();
  ASSERT_EQ(2, in_memory_persistency->getStartingIndex());
  ASSERT_EQ(2, in_memory_persistency->getEndingIndex());
}


TEST_F(RocksDBPersistencyTest, basic)
{
  auto rocksdb_persistency = std::make_unique<RocksDBPersistency>(tmp_dir.string());
  ASSERT_EQ(0, rocksdb_persistency->getStartingIndex());
  ASSERT_EQ(0, rocksdb_persistency->getEndingIndex());
  q_item_t out;
  q_item_t in{"hello", "world"};
  rocksdb_persistency->record(0, in);
  EXPECT_EQ(0, rocksdb_persistency->getStartingIndex());
  EXPECT_EQ(1, rocksdb_persistency->getEndingIndex());
  EXPECT_TRUE(rocksdb_persistency->retrieve(0,out));
  EXPECT_EQ(in, out);

  rocksdb_persistency->pop();
  EXPECT_EQ(1, rocksdb_persistency->getStartingIndex());
  EXPECT_EQ(1, rocksdb_persistency->getEndingIndex());
}


TEST_F(RocksDBPersistencyTest, Pop)
{
  auto rocksdb_persistency = std::make_unique<RocksDBPersistency>(tmp_dir.string());
  ASSERT_EQ(0, rocksdb_persistency->getStartingIndex());
  ASSERT_EQ(0, rocksdb_persistency->getEndingIndex());
  rocksdb_persistency->record(0, {"hello", "world"});
  rocksdb_persistency->record(1, {"foo", "bar"});

  ASSERT_EQ(0, rocksdb_persistency->getStartingIndex());
  ASSERT_EQ(2, rocksdb_persistency->getEndingIndex());
  rocksdb_persistency->pop();
  ASSERT_EQ(1, rocksdb_persistency->getStartingIndex());
  ASSERT_EQ(2, rocksdb_persistency->getEndingIndex());
  rocksdb_persistency->pop();
  ASSERT_EQ(2, rocksdb_persistency->getStartingIndex());
  ASSERT_EQ(2, rocksdb_persistency->getEndingIndex());
}

TEST_F(RocksDBDeathTest, corrupt_record)
{
  GTEST_FLAG_SET(death_test_style, "fast");
  ASSERT_DEATH({
    auto rocksdb_persistency = std::make_unique<RocksDBPersistency>(tmp_dir.string());
    ASSERT_EQ(0, rocksdb_persistency->getStartingIndex());
    ASSERT_EQ(0, rocksdb_persistency->getEndingIndex());
    rocksdb_persistency->record(42, {"hello", "world"});
  }, "Queue corruption, received unexpected index: 42");
}

TEST_F(RocksDBDeathTest , pop_empty)
{
  GTEST_FLAG_SET(death_test_style, "fast");
  ASSERT_DEATH({
    auto rocksdb_persistency = std::make_unique<RocksDBPersistency>(tmp_dir.string());
    ASSERT_EQ(0, rocksdb_persistency->getStartingIndex());
    ASSERT_EQ(0, rocksdb_persistency->getEndingIndex());
    rocksdb_persistency->pop();
  }, "Queue corruption, cannot pop item. startIndex = 0, endIndex = 0");
}

TEST_F(RocksDBDeathTest, stress_corrupt_record)
{

  GTEST_FLAG_SET(death_test_style, "fast");
  EXPECT_DEATH( {
    auto rocksdb_persistency = std::make_unique<RocksDBPersistency>(tmp_dir.string());
    ASSERT_EQ(0, rocksdb_persistency->getStartingIndex());
    ASSERT_EQ(0, rocksdb_persistency->getEndingIndex());


    auto write_fn = [&rocksdb_persistency](){
      for(int i = 0; i < 10000; i++) {
        rocksdb_persistency->record(rocksdb_persistency->getEndingIndex(),
                                    {"ctr", std::to_string(i)});
      }
    };

    std::vector<std::thread> writers;
    for (int i = 0; i < 16; i++) {
      writers.push_back(std::thread(write_fn));
    }

    for (auto& writer_thread : writers) {
      writer_thread.join();
    }
  }, "Queue corruption, received unexpected index: ");

}
