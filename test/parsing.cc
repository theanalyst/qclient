// ----------------------------------------------------------------------
// File: parsing.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "qclient/ResponseParsing.hh"
#include "qclient/ResponseBuilder.hh"
#include <gtest/gtest.h>

using namespace qclient;

TEST(ResponseParsing, StatusParserErr) {
  redisReplyPtr reply = ResponseBuilder::makeStr("test test");

  StatusParser parser(reply);
  ASSERT_FALSE(parser.ok());
  ASSERT_TRUE(parser.value().empty());
  ASSERT_EQ(parser.err(), "Unexpected reply type; was expecting STATUS, received \"test test\"");

  StatusParser parser2(reply.get());
  ASSERT_FALSE(parser2.ok());
  ASSERT_TRUE(parser2.value().empty());
  ASSERT_EQ(parser2.err(), "Unexpected reply type; was expecting STATUS, received \"test test\"");
}

TEST(ResponseParsing, StatusParser) {
  redisReplyPtr reply = ResponseBuilder::makeStatus("some status");

  StatusParser parser(reply);
  ASSERT_TRUE(parser.ok());
  ASSERT_TRUE(parser.err().empty());
  ASSERT_EQ(parser.value(), "some status");
}

TEST(ResponseParsing, IntegerParserErr) {
  redisReplyPtr reply = ResponseBuilder::makeStatus("aaa");

  IntegerParser parser(reply);
  ASSERT_FALSE(parser.ok());
  ASSERT_EQ(parser.err(), "Unexpected reply type; was expecting INTEGER, received aaa");
}

TEST(ResponseParsing, IntegerParser) {
  redisReplyPtr reply = ResponseBuilder::makeInt(13);

  IntegerParser parser(reply);
  ASSERT_TRUE(parser.ok());
  ASSERT_EQ(parser.value(), 13);
}

TEST(ResponseParsing, StringParser) {
  redisReplyPtr reply = ResponseBuilder::makeStr("turtles");

  StringParser parser(reply);
  ASSERT_TRUE(parser.ok());
  ASSERT_TRUE(parser.err().empty());
  ASSERT_EQ(parser.value(), "turtles");
}

TEST(ResponseParsing, StringParserErr) {
  redisReplyPtr reply = ResponseBuilder::makeInt(13);

  StringParser parser(reply);
  ASSERT_FALSE(parser.ok());
  ASSERT_EQ(parser.err(), "Unexpected reply type; was expecting STRING, received (integer) 13");
}

TEST(ResponseParsing, HgetallParserNull) {
  HgetallParser parser(nullptr);
  ASSERT_FALSE(parser.ok());
  ASSERT_EQ(parser.err(), "Received null redisReply");
}

TEST(ResponseParsing, HgetallParserErrInt) {
  HgetallParser parser(ResponseBuilder::makeInt(13));
  ASSERT_FALSE(parser.ok());
  ASSERT_EQ(parser.err(), "Unexpected reply type; was expecting ARRAY, received (integer) 13");
}

TEST(ResponseParsing, HgetallParserErrOddElements) {
  std::vector<std::string> vec = { "1", "2", "3" };

  HgetallParser parser(ResponseBuilder::makeStringArray(vec));
  ASSERT_FALSE(parser.ok());
  ASSERT_EQ(parser.err(), "Unexpected number of elements; expected a multiple of 2, received 3");
}

TEST(ResponseParsing, HgetallParserErrBadTypesInArray) {
  ResponseBuilder builder;
  builder.feed("*2\r\n$1\r\na\r\n+3\r\n");

  redisReplyPtr reply;
  ASSERT_EQ(ResponseBuilder::Status::kOk, builder.pull(reply));

  HgetallParser parser(reply);
  ASSERT_FALSE(parser.ok());
  ASSERT_EQ(parser.err(), "Unexpected reply type for element #1: Unexpected reply type; was expecting STRING, received 3");
}

TEST(ResponseParsing, HgetallParserEmpty) {
  std::vector<std::string> emptyVec;

  HgetallParser parser(ResponseBuilder::makeStringArray(emptyVec));
  ASSERT_TRUE(parser.ok());
  ASSERT_TRUE(parser.err().empty());
  ASSERT_TRUE(parser.value().empty());
}

TEST(ResponseParsing, HgetallDuplicate) {
  std::vector<std::string> vec = { "1", "2", "1", "4" };

  HgetallParser parser(ResponseBuilder::makeStringArray(vec));
  ASSERT_FALSE(parser.ok());
  ASSERT_EQ(parser.err(), "Found duplicate key: '1'");
}

TEST(ResponseParsing, Hgetall) {
  std::vector<std::string> vec = { "1", "2", "3", "4" };

  HgetallParser parser(ResponseBuilder::makeStringArray(vec));
  ASSERT_TRUE(parser.ok());
  ASSERT_TRUE(parser.err().empty());

  std::map<std::string, std::string> val = parser.value();
  ASSERT_EQ(val.size(), 2u);
  ASSERT_EQ(val["1"], "2");
  ASSERT_EQ(val["3"], "4");
}
