// ----------------------------------------------------------------------
// File: formatting.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
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

#include "gtest/gtest.h"
#include "qclient/QClient.hh"
using namespace qclient;

void setStr(redisReplyPtr reply, const std::string &str) {
  reply->str = (char*) str.c_str(); // evil const cast
  reply->len = str.size();
}

TEST(DescribeRedisReply, BasicSanity1) {
  ASSERT_EQ(describeRedisReply(redisReplyPtr()), "nullptr");

  redisReplyPtr reply = redisReplyPtr((redisReply*) malloc(sizeof(redisReply)), freeReplyObject);

  reply->type = REDIS_REPLY_NIL;
  ASSERT_EQ(describeRedisReply(reply), "(nil)");

  reply->type = REDIS_REPLY_INTEGER;
  reply->integer = 13;
  ASSERT_EQ(describeRedisReply(reply), "(integer) 13");

  std::string str = "OK";
  reply->type = REDIS_REPLY_STATUS;
  setStr(reply, str);
  ASSERT_EQ(describeRedisReply(reply), "OK");

  reply->type = REDIS_REPLY_STRING;
  ASSERT_EQ(describeRedisReply(reply), "\"OK\"");

  str = "abc111";
  str.push_back('\0');
  str.push_back('\0');
  str += "\xAB" "aaaaaaa";
  setStr(reply, str);

  ASSERT_EQ(describeRedisReply(reply), "\"abc111\\x00\\x00\\xABaaaaaaa\"");
  reply->str = nullptr;

  redisReader* reader = redisReaderCreate();
  str = "*2\r\n$6\r\nnext:d\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
  redisReaderFeed(reader, str.c_str(), str.size());
  redisReply *rep = nullptr;
  redisReaderGetReply(reader, (void**) &rep);
  ASSERT_NE(rep, nullptr);

  std::cout << describeRedisReply(rep) << std::endl;
  ASSERT_EQ(describeRedisReply(rep), "1) \"next:d\"\n2) 1) \"a\"\n   2) \"b\"\n   3) \"c\"\n");
  freeReplyObject(rep);

  str = "*2\r\n$6\r\nnext:d\r\n*3\r\n*2\r\n:1337\r\n$2\r\nbb\r\n$1\r\nb\r\n$1\r\nc\r\n";
  redisReaderFeed(reader, str.c_str(), str.size());
  redisReaderGetReply(reader, (void**) &rep);
  ASSERT_NE(rep, nullptr);

  std::cout << describeRedisReply(rep) << std::endl;
  ASSERT_EQ(describeRedisReply(rep), "1) \"next:d\"\n2) 1) 1) (integer) 1337\n      2) \"bb\"\n   2) \"b\"\n   3) \"c\"\n");
  freeReplyObject(rep);

  str = "*2\r\n$6\r\nnext:d\r\n*0\r\n";
  redisReaderFeed(reader, str.c_str(), str.size());
  redisReaderGetReply(reader, (void**) &rep);
  ASSERT_NE(rep, nullptr);

  std::cout << describeRedisReply(rep);
  ASSERT_EQ(describeRedisReply(rep), "1) \"next:d\"\n2) (empty list or set)\n");
  freeReplyObject(rep);

  redisReaderFree(reader);

  reply->type = 999;
  ASSERT_EQ(describeRedisReply(reply), "!!! unknown reply type !!!");
}
