//------------------------------------------------------------------------------
// File: set.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include <gtest/gtest.h>
#include "qclient/QSet.hh"
#include <set>
#include <algorithm>

using namespace qclient;
static std::string sHost = "localhost";
static int sPort = 7777;

//------------------------------------------------------------------------------
// Test Set class - synchronous
//------------------------------------------------------------------------------
TEST(QSet, SetSync)
{
  QClient cl{sHost, sPort};
  std::string set_key = "qclient_test:set_sync";
  QSet qset(cl, set_key);
  std::vector<std::string> members = {"200", "300", "400"};
  ASSERT_EQ(members.size(), qset.sadd(members));
  ASSERT_TRUE(qset.sadd("100"));
  ASSERT_FALSE(qset.sadd("400"));
  (void) members.push_back("100");
  ASSERT_TRUE(qset.sismember("300"));
  ASSERT_FALSE(qset.sismember("1500"));
  ASSERT_EQ(4, qset.scard());
  std::set<std::string> ret_members = qset.smembers();

  for (const auto& elem : members) {
    ASSERT_TRUE(ret_members.find(elem) != ret_members.end());
  }

  ASSERT_TRUE(qset.srem("100"));
  ASSERT_EQ(3, qset.srem(members));
  ASSERT_FALSE(qset.srem("100"));
  ASSERT_EQ(0, qset.scard());
  // Test SSCAN functionality
  members.clear();

  for (int i = 0; i < 3000; ++i) {
    members.push_back(std::to_string(i));
    ASSERT_TRUE(qset.sadd(std::to_string(i)));
  }

  std::string cursor = "0";
  long long count = 1000;
  std::pair< std::string, std::vector<std::string> > reply;

  do {
    reply = qset.sscan(cursor, count);
    cursor = reply.first;

    for (auto && elem : reply.second) {
      ASSERT_TRUE(std::find(members.begin(), members.end(), elem) !=
		  members.end());
    }
  } while (cursor != "0");

  auto future = cl.execute({"DEL", set_key});
  ASSERT_EQ(1, future.get()->integer);
}

//------------------------------------------------------------------------------
// Test Set class - asynchronous
//------------------------------------------------------------------------------
/*
TEST(QSet, SetAsync) {
  connect();
  std::string set_key = "qclient_test:set_async";
  QSet qset(cl,set_key);

  std::vector<std::string> members = {"200", "300", "400"};
  std::list<std::string> lst_errors;
  std::atomic<std::uint64_t> num_asyn_req {0};
  std::mutex mutex;
  std::condition_variable cond_var;
  auto callback = [&](Command<int>& c) {
    if ((c.ok() && c.reply() != 1) || !c.ok()) {
      std::ostringstream oss;
      oss << "Failed command: " << c.cmd() << " error: " << c.lastError();
      lst_errors.emplace(lst_errors.end(), oss.str());
    }

    if (--num_asyn_req == 0) {
      cond_var.notify_one();
    }
  };

  std::string value;
  // Add some elements
  for (auto i = 0; i < 100; ++i) {
    value = "val" + std::to_string(i);
    num_asyn_req++;
    qset.sadd(value, callback);
  }

  // Wait for all the replies
  {
    std::unique_lock<std::mutex> lock(mutex);
    while (num_asyn_req) {
      cond_var.wait(lock);
    }
  }

  ASSERT_EQ(0, lst_errors.size());
  // Add some more elements that will trigger some errors
  for (auto i = 90; i < 110; ++i) {
    value = "val" + std::to_string(i);
    num_asyn_req++;
    qset.sadd(value, callback);
  }

  // Wait for all the replies
  {
    std::unique_lock<std::mutex> lock(mutex);
    while (num_asyn_req) {
      cond_var.wait(lock);
    }
  }

  ASSERT_EQ(10, lst_errors.size());
  ASSERT_EQ(110, qset.scard());
  lst_errors.clear();

  // Remove all elements
  for (auto i = 0; i < 110; ++i)
  {
    num_asyn_req++;
    value = "val" + std::to_string(i);
    qset.srem(value, callback);
  }

  // Wait for all the replies
  {
    std::unique_lock<std::mutex> lock(mutex);
    while (num_asyn_req) {
      cond_var.wait(lock);
    }
  }

  ASSERT_EQ(0, lst_errors.size());
  auto future = cl.execute({"DEL", set_key});
  ASSERT_EQ(1, future.get()->integer);
}
*/
