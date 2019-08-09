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
#include "qclient/Formatting.hh"
#include "qclient/ResponseBuilder.hh"
#include "shared/SharedSerialization.hh"

using namespace qclient;

void setStr(redisReplyPtr reply, const std::string &str) {
  reply->str = (char*) str.c_str(); // evil const cast
  reply->len = str.size();
}

TEST(DescribeRedisReply, BasicSanity1) {
  ASSERT_EQ(describeRedisReply(redisReplyPtr()), "nullptr");

  redisReplyPtr reply = redisReplyPtr(new redisReply());

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

  std::string description;

  description = qclient::describeRedisReply("*2\r\n$6\r\nnext:d\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
  std::cout << description << std::endl;
  ASSERT_EQ(description, "1) \"next:d\"\n2) 1) \"a\"\n   2) \"b\"\n   3) \"c\"\n");

  description = qclient::describeRedisReply("*2\r\n$6\r\nnext:d\r\n*3\r\n*2\r\n:1337\r\n$2\r\nbb\r\n$1\r\nb\r\n$1\r\nc\r\n");
  std::cout << description << std::endl;
  ASSERT_EQ(description, "1) \"next:d\"\n2) 1) 1) (integer) 1337\n      2) \"bb\"\n   2) \"b\"\n   3) \"c\"\n");

  description = qclient::describeRedisReply("*2\r\n$6\r\nnext:d\r\n*0\r\n");
  std::cout << description << std::endl;
  ASSERT_EQ(description, "1) \"next:d\"\n2) (empty list or set)\n");

  reply->type = 999;
  ASSERT_EQ(describeRedisReply(reply), "!!! unknown reply type !!!");
}

TEST(Formatting, SerializeString) {
  ASSERT_EQ(Formatting::serialize("asdf"), "$4\r\nasdf\r\n");
}

TEST(Formatting, SerializeVector) {
  ASSERT_EQ(Formatting::serializeVector("asdf", "bbb", "aaaa"),
    "*3\r\n"
    "$4\r\nasdf\r\n"
    "$3\r\nbbb\r\n"
    "$4\r\naaaa\r\n"
  );

  ASSERT_EQ(Formatting::serializeVector("asdf", 1234),
    "*2\r\n"
    "$4\r\nasdf\r\n"
    ":1234\r\n"
  );
}

TEST(Formatting, SerializeIntVector) {
  std::vector<int64_t> vec = {4, 9, 8};

  ASSERT_EQ(Formatting::serialize(vec),
    "*3\r\n"
    ":4\r\n"
    ":9\r\n"
    ":8\r\n"
  );

  redisReplyPtr reply = ResponseBuilder::parseRedisEncodedString(Formatting::serialize(vec));
  ASSERT_EQ(describeRedisReply(reply),
    "1) (integer) 4\n"
    "2) (integer) 9\n"
    "3) (integer) 8\n"
  );
}

TEST(Formatting, SerializeStringMap) {
  std::map<std::string, std::string> map;
  map["i like"] = "pickles";
  map["asdf"] = "1234";

  ASSERT_EQ(Formatting::serialize(map),
    "*4\r\n"
    "$4\r\nasdf\r\n"
    "$4\r\n1234\r\n"
    "$6\r\ni like\r\n"
    "$7\r\npickles\r\n"
  );
}

TEST(Formatting, DescribeEncodedString) {
  ASSERT_EQ(
    "(integer) 5",
    qclient::ResponseBuilder::parseAndDescribeRedisEncodedString(":5\r\n")
  );

  ASSERT_EQ(
    "nullptr",
    qclient::ResponseBuilder::parseAndDescribeRedisEncodedString("aaaaaaaaaa")
  );
}

TEST(SharedSerialization, BatchUpdate) {
  std::map<std::string, std::string> batch;
  batch["a"] = "bb";
  batch["ccc"] = "dddd";
  batch["eeeee"] = "ffffff";

  std::map<std::string, std::string> parsed;
  ASSERT_TRUE(qclient::parseBatch(qclient::serializeBatch(batch), parsed));
  ASSERT_EQ(batch, parsed);
}
