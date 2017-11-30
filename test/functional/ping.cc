//------------------------------------------------------------------------------
// File: ping.cc
// Author: Georgios Bitzes - CERN
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
#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

//------------------------------------------------------------------------------
// Simply ping the server
//------------------------------------------------------------------------------
TEST(Ping, one)
{
  QClient cl{testconfig.host, testconfig.port, false, {}, testconfig.tlsconfig};

  redisReplyPtr reply = cl.exec("PING", "hello there").get();
  ASSERT_TRUE(reply != nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_STRING);
  ASSERT_GT(reply->len, 0);

  ASSERT_EQ(strcmp(reply->str, "hello there"), 0);
}

TEST(Ping, pipelined_10k) {
  std::vector<std::future<redisReplyPtr>> responses;

  QClient cl{testconfig.host, testconfig.port, false, {}, testconfig.tlsconfig};

  for(size_t i = 0; i < 10000; i++) {
    responses.push_back(cl.exec("PING", SSTR("hello from ping #" << i)));
  }

  for(size_t i = 0; i < 10000; i++) {
    redisReplyPtr reply = responses[i].get();
    ASSERT_TRUE(reply != nullptr);
    ASSERT_EQ(reply->type, REDIS_REPLY_STRING);
    ASSERT_EQ(std::string(reply->str, reply->len), SSTR("hello from ping #" << i));
  }
}
