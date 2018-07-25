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
  qclient::Endpoint e1("example.com", 1234);
  qclient::Endpoint e2("localhost", 999);
  qclient::Endpoint e3("localhost", 998);

  qclient::GlobalInterceptor::addIntercept(e1, e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e1), e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e2), e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e3), e3);

  qclient::GlobalInterceptor::clearIntercepts();
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e1), e1);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e2), e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e3), e3);
}

TEST(EncodedRequest, BasicSanity) {
  std::vector<std::string> req { "set", "1234", "abc" };
  qclient::EncodedRequest encoded(req);

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
  qclient::RequestStager stager(qclient::BackpressureStrategy::Default());

  std::future<redisReplyPtr> fut1 = stager.stage(EncodedRequest( std::vector<std::string>{"ping", "asdf1"} ));
  std::future<redisReplyPtr> fut2 = stager.stage(EncodedRequest( std::vector<std::string>{"ping", "asdf2"} ));
  std::future<redisReplyPtr> fut3 = stager.stage(EncodedRequest( std::vector<std::string>{"ping", "asdf3"} ));

  stager.satisfy(ResponseBuilder::makeInt(5));
  stager.satisfy(ResponseBuilder::makeInt(7));
  stager.satisfy(ResponseBuilder::makeInt(9));

  ASSERT_REPLY(fut1, 5);
  ASSERT_REPLY(fut2, 7);
  ASSERT_REPLY(fut3, 9);
}
