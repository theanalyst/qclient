// ----------------------------------------------------------------------
// File: general.cc
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
#include "qclient/GlobalInterceptor.hh"
#include "qclient/EncodedRequest.hh"
#include "qclient/ResponseBuilder.hh"
#include "RequestStager.hh"
#include "ReplyMacros.hh"

using namespace qclient;

TEST(GlobalInterceptor, BasicSanity) {
  Endpoint e1("example.com", 1234);
  Endpoint e2("localhost", 999);
  Endpoint e3("localhost", 998);

  GlobalInterceptor::addIntercept(e1, e2);
  ASSERT_EQ(GlobalInterceptor::translate(e1), e2);
  ASSERT_EQ(GlobalInterceptor::translate(e2), e2);
  ASSERT_EQ(GlobalInterceptor::translate(e3), e3);

  GlobalInterceptor::clearIntercepts();
  ASSERT_EQ(GlobalInterceptor::translate(e1), e1);
  ASSERT_EQ(GlobalInterceptor::translate(e2), e2);
  ASSERT_EQ(GlobalInterceptor::translate(e3), e3);
}

TEST(EncodedRequest, BasicSanity) {
  std::vector<std::string> req { "set", "1234", "abc" };
  EncodedRequest encoded(req);
  ASSERT_EQ("*3\r\n$3\r\nset\r\n$4\r\n1234\r\n$3\r\nabc\r\n", std::string(encoded.getBuffer(), encoded.getLen()));
}

TEST(ResponseBuilder, BasicSanity) {
  ResponseBuilder builder;

  builder.feed("ayy-lmao");

  redisReplyPtr reply;
  ASSERT_EQ(builder.pull(reply), ResponseBuilder::Status::kProtocolError);
  ASSERT_EQ(builder.pull(reply), ResponseBuilder::Status::kProtocolError);

  builder.restart();
  ASSERT_EQ(builder.pull(reply), ResponseBuilder::Status::kIncomplete);

  builder.feed(":10\r");
  ASSERT_EQ(builder.pull(reply), ResponseBuilder::Status::kIncomplete);
  builder.feed("\n");
  ASSERT_EQ(builder.pull(reply), ResponseBuilder::Status::kOk);

  ASSERT_EQ(reply->type, REDIS_REPLY_INTEGER);
  ASSERT_EQ(reply->integer, 10);
}

TEST(RequestStager, BasicSanity) {
  RequestStager stager(BackpressureStrategy::Default());

  std::future<redisReplyPtr> fut1 = stager.stage(EncodedRequest::make("ping", "asdf1"));
  std::future<redisReplyPtr> fut2 = stager.stage(EncodedRequest::make("ping", "asdf2"));
  std::future<redisReplyPtr> fut3 = stager.stage(EncodedRequest::make("ping", "asdf3"));

  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(5)));
  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(9)));

  ASSERT_REPLY(fut1, 5);
  ASSERT_REPLY(fut2, 7);
  ASSERT_REPLY(fut3, 9);
}

TEST(RequestStager, Overflow) {
  RequestStager stager(BackpressureStrategy::Default());

  std::future<redisReplyPtr> fut1 = stager.stage(EncodedRequest::make("ping", "123"));

  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_FALSE(stager.consumeResponse(ResponseBuilder::makeInt(7))); // server sent an extra response, not good
}

TEST(RequestStager, IgnoredResponses) {
  RequestStager stager(BackpressureStrategy::Default());

  std::future<redisReplyPtr> fut1 = stager.stage(EncodedRequest::make("ping", "1234"), false, 1);

  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_REPLY(fut1, 8);
}

TEST(RequestStager, IgnoredResponsesWithReconnect) {
  RequestStager stager(BackpressureStrategy::Default());

  std::future<redisReplyPtr> fut1 = stager.stage(EncodedRequest::make("ping", "789"), false, 2);

  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  stager.reconnection();

  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(9)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  ASSERT_TRUE(stager.consumeResponse(ResponseBuilder::makeInt(3)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  ASSERT_REPLY(fut1, 3);
}
