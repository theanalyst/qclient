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
#include "test-config.hh"
#include "qclient/QSet.hh"
#include "qclient/AsyncHandler.hh"
#include <set>
#include <list>
#include <algorithm>

using namespace qclient;

//------------------------------------------------------------------------------
// Test Set class - synchronous
//------------------------------------------------------------------------------
TEST(QSet, SetSync)
{
  QClient cl{testconfig.host, testconfig.port, {} };
  std::string set_key = "qclient_test:set_sync";
  QSet qset(cl, set_key);
  std::list<std::string> members = {"200", "300", "400"};
  ASSERT_EQ(int(members.size()), qset.sadd(members));
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

  auto future = cl.execute(std::vector<std::string>({"DEL", set_key}));
  ASSERT_EQ(1, future.get()->integer);
}

//------------------------------------------------------------------------------
// Test Set class - asynchronous
//------------------------------------------------------------------------------
TEST(QSet, SetAsync)
{
  QClient cl{testconfig.host, testconfig.port, {} };
  std::string set_key = "qclient_test:set_async";
  QSet qset(cl, set_key);
  qclient::AsyncHandler ah;
  std::string value;

  // Add some elements
  for (auto i = 0; i < 100; ++i) {
    value = "val" + std::to_string(i);
    qset.sadd_async(value, &ah);
  }

  ASSERT_TRUE(ah.Wait());

  // Add some more elements that will trigger some errors
  for (auto i = 90; i < 110; ++i) {
    value = "val" + std::to_string(i);
    qset.sadd_async(value, &ah);
  }

  // Wait for all the replies
  ASSERT_TRUE(ah.Wait());
  std::list<long long int> resp = ah.GetResponses();
  ASSERT_EQ(10, std::count_if(resp.begin(), resp.end(),
                              [](long long int elem) {
                                return (elem != 1);
                              }));
  ASSERT_EQ(110, qset.scard());

  // Remove all elements
  for (auto i = 0; i < 110; ++i) {
    value = "val" + std::to_string(i);
    qset.srem_async(value, &ah);
  }

  ASSERT_TRUE(ah.Wait());
  resp = ah.GetResponses();
  ASSERT_EQ(0, std::count_if(resp.begin(), resp.end(),
                             [](long long int elem) {
                               return (elem != 1);
                             }));

  // Bulk asynchronous add elements
  std::set<std::string> set_elem;

  for (auto i = 500; i < 600; ++i) {
    value = "val" + std::to_string(i);
    set_elem.insert(value);
  }

  qset.sadd_async(set_elem, &ah);
  ASSERT_TRUE(ah.Wait());
  resp = ah.GetResponses();
  ASSERT_EQ(100, *(resp.begin()));
  auto future = cl.execute(std::vector<std::string>({"DEL", set_key}));
  ASSERT_EQ(1, future.get()->integer);
}
