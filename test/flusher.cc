// ----------------------------------------------------------------------
// File: flusher.cc
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
  * qclient - A simple redis C++ client with support for redirects       *
  * Copyright (C) 2024 CERN/Switzerland                                  *
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
#include "QDBFixture.hh"
#include "qclient/BackgroundFlusher.hh"
#include "qclient/RocksDBPersistency.hh"

using namespace qclient;

TEST_F(QDBInstance, FlusherConstructMemoryPersistency)
{
  qclient::BackgroundFlusher flusher(members, getQCOpts(),
                                     dummyNotifier,
                                     new qclient::StubInMemoryPersistency<q_item_t>());
  ASSERT_EQ(0, flusher.getStartingIndex());
  ASSERT_EQ(0, flusher.getEndingIndex());
}

TEST_F(QDBInstance, FlusherConstructRocksDBPersistency)
{
  qclient::BackgroundFlusher flusher(members, getQCOpts(),
                                     dummyNotifier,
                                     new qclient::RocksDBPersistency(tmp_dir));
  ASSERT_EQ(0, flusher.getStartingIndex());
  ASSERT_EQ(0, flusher.getEndingIndex());
}

void testPush(qclient::BackgroundFlusher& flusher,
              std::string tag="rocksdb")
{
  auto start_time = std::chrono::high_resolution_clock::now();
  int max_reqs = 10000;
  std::string hash_str = "dict_" + tag + "_single";
  for (int i = 0; i < max_reqs; i++) {
    std::string key = "key" + std::to_string(i);
    std::string val = "val" + std::to_string(i);
    flusher.pushRequest({"HSET", hash_str, key, val});
  }

  EXPECT_EQ(max_reqs, flusher.getEndingIndex());

  while (!flusher.waitForIndex(max_reqs - 1, std::chrono::milliseconds(10))) {
    std::cerr << "total pending=" << flusher.size() << " enqueued = " << flusher.getEnqueuedAndClear()
              << " acknowledged = " << flusher.getAcknowledgedAndClear() << std::endl;
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  auto ms_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  EXPECT_EQ(max_reqs, flusher.getStartingIndex());
  std::cerr << "Total test time with " << tag << " persistency for "
            << max_reqs << " serial requests:" << ms_elapsed
            << " ms" << std::endl;
}

void testMultiPush(qclient::BackgroundFlusher& flusher,
                   std::string tag="rocksdb")
{
  auto start_time = std::chrono::high_resolution_clock::now();
  int max_reqs = 10000;
  int max_threads = 16;

  auto push_fn = [&flusher, &tag, max_reqs](int thread_id) {
    std::string hash_str = "dict_" + tag +  std::to_string(thread_id);
    for (int i = 0; i < max_reqs; i++) {
      std::string key = "key" + std::to_string(i);
      std::string val = "val" + std::to_string(i);
      flusher.pushRequest({"HSET", hash_str, key, val});
    }
  };

  std::vector<std::thread> threads;
  for (int i=0; i < max_threads; ++i) {
    threads.push_back(std::thread(push_fn, i));
  }

  for (auto& thread : threads) {
    thread.join();
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  auto ms_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  EXPECT_EQ(max_reqs * max_threads, flusher.getEndingIndex());
  while (!flusher.waitForIndex(max_reqs*max_threads - 1, std::chrono::milliseconds(100))) {
   std::cerr << "total pending=" << flusher.size() << " enqueued = " << flusher.getEnqueuedAndClear()
             << " acknowledged = " << flusher.getAcknowledgedAndClear()
             << " starting index =" << flusher.getStartingIndex()
             << " ending index =" << flusher.getEndingIndex() << std::endl;
  }
  EXPECT_EQ(max_reqs * max_threads, flusher.getStartingIndex());
  std::cerr << "Total test time with " << tag << " persistency for " <<
             max_reqs << " reqs/thread with " << max_threads
            << " threads " << ms_elapsed
            << " ms frequency " << (max_reqs * max_threads)/ms_elapsed << " kHz"
            << std::endl;
}

TEST_F(QDBFlusherInstance, MemoryPersistencypush)
{
  qclient::BackgroundFlusher flusher(members, getQCOpts(),
                                     dummyNotifier,
                                     new qclient::StubInMemoryPersistency<q_item_t>());
  testPush(flusher, "memory");
}

TEST_F(QDBFlusherInstance, MemoryPersistencyMultiPush)
{
  qclient::BackgroundFlusher flusher(members, getQCOpts(),
                                     dummyNotifier,
                                     new qclient::StubInMemoryPersistency<q_item_t>());
  testMultiPush(flusher, "memory");
}

TEST_F(QDBFlusherInstance, MemoryPersistencyMultiPushLockfree)
{
  qclient::BackgroundFlusher flusher(members, getQCOpts(),
                                     dummyNotifier,
                                     new qclient::StubInMemoryPersistency<q_item_t>(),
                                     qclient::FlusherQueueHandler::LockFree);
  testMultiPush(flusher, "memory");
}

TEST_F(QDBFlusherInstance, RocksDBPeristencypush)
{
  qclient::BackgroundFlusher flusher(members, getQCOpts(),
                                     dummyNotifier,
                                     new qclient::RocksDBPersistency(tmp_dir));
  testPush(flusher, "rocksdb");
}

TEST_F(QDBFlusherInstance, RocksDBPersistencyMultiPush)
{
  qclient::BackgroundFlusher flusher(members, getQCOpts(),
                                     dummyNotifier,
                                     new qclient::RocksDBPersistency(tmp_dir));
  testMultiPush(flusher, "rocksdb");
}
