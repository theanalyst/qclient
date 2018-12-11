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

#include "EndpointDecider.hh"
#include "qclient/GlobalInterceptor.hh"
#include "qclient/EncodedRequest.hh"
#include "qclient/ResponseBuilder.hh"
#include "qclient/MultiBuilder.hh"
#include "qclient/Handshake.hh"
#include "qclient/pubsub/MessageQueue.hh"
#include "ConnectionHandler.hh"
#include "ReplyMacros.hh"

#include "gtest/gtest.h"
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

TEST(EncodedRequest, FusedEncodedRequest) {
  std::deque<EncodedRequest> reqs;

  reqs.emplace_back(EncodedRequest::make("ping", "124"));
  reqs.emplace_back(EncodedRequest::make("ping", "4321"));
  reqs.emplace_back(EncodedRequest::make("set", "abc", "1234"));

  EncodedRequest fused = EncodedRequest::fuseIntoBlock(reqs);
  ASSERT_EQ("*2\r\n$4\r\nping\r\n$3\r\n124\r\n*2\r\n$4\r\nping\r\n$4\r\n4321\r\n*3\r\n$3\r\nset\r\n$3\r\nabc\r\n$4\r\n1234\r\n", std::string(fused.getBuffer(), fused.getLen()));
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

TEST(ResponseBuilder, MakeErr) {
  redisReplyPtr reply = ResponseBuilder::makeErr("UNAVAILABLE test");
  ASSERT_NE(reply, nullptr);

  ASSERT_EQ(reply->type, REDIS_REPLY_ERROR);
  ASSERT_EQ(std::string(reply->str, reply->len), "UNAVAILABLE test");
}

TEST(ResponseBuilder, MakeStr) {
  redisReplyPtr reply = ResponseBuilder::makeStr("test test 123");
  ASSERT_NE(reply, nullptr);

  ASSERT_EQ(reply->type, REDIS_REPLY_STRING);
  ASSERT_EQ(std::string(reply->str, reply->len), "test test 123");
}

TEST(ResponseBuilder, MakeStringArray) {
  redisReplyPtr reply = ResponseBuilder::makeStringArray( {"test", "abc", "asdf"} );
  ASSERT_NE(reply, nullptr);

  ASSERT_EQ(reply->type, REDIS_REPLY_ARRAY);
  ASSERT_EQ(reply->elements, 3u);

  ASSERT_EQ(reply->element[0]->type, REDIS_REPLY_STRING);
  ASSERT_EQ(reply->element[1]->type, REDIS_REPLY_STRING);
  ASSERT_EQ(reply->element[2]->type, REDIS_REPLY_STRING);

  ASSERT_EQ(std::string(reply->element[0]->str, reply->element[0]->len), "test");
  ASSERT_EQ(std::string(reply->element[1]->str, reply->element[1]->len), "abc");
  ASSERT_EQ(std::string(reply->element[2]->str, reply->element[2]->len), "asdf");
}

TEST(ResponseBuilder, MakeArrayStrStrInt) {
  redisReplyPtr reply = ResponseBuilder::makeArr("element1", "element2", 7);
  ASSERT_NE(reply, nullptr);

  ASSERT_EQ(reply->type, REDIS_REPLY_ARRAY);
  ASSERT_EQ(reply->elements, 3u);

  ASSERT_EQ(reply->element[0]->type, REDIS_REPLY_STRING);
  ASSERT_EQ(reply->element[1]->type, REDIS_REPLY_STRING);
  ASSERT_EQ(reply->element[2]->type, REDIS_REPLY_INTEGER);

  ASSERT_EQ(std::string(reply->element[0]->str, reply->element[0]->len), "element1");
  ASSERT_EQ(std::string(reply->element[1]->str, reply->element[1]->len), "element2");
  ASSERT_EQ(reply->element[2]->integer, 7);
}

TEST(ConnectionHandler, NoRetries) {
  ConnectionHandler handler(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::NoRetries());

  std::future<redisReplyPtr> fut1 = handler.stage(EncodedRequest::make("ping", "123"));
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeErr("UNAVAILABLE test test")));

  ASSERT_REPLY(fut1, "UNAVAILABLE test test");
}

TEST(ConnectionHandler, BasicSanity) {
  ConnectionHandler handler(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = handler.stage(EncodedRequest::make("ping", "asdf1"));
  std::future<redisReplyPtr> fut2 = handler.stage(EncodedRequest::make("ping", "asdf2"));
  std::future<redisReplyPtr> fut3 = handler.stage(EncodedRequest::make("ping", "asdf3"));

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(5)));
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(9)));

  ASSERT_REPLY(fut1, 5);
  ASSERT_REPLY(fut2, 7);
  ASSERT_REPLY(fut3, 9);
}

TEST(ConnectionHandler, Overflow) {
  ConnectionHandler handler(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = handler.stage(EncodedRequest::make("ping", "123"));

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_FALSE(handler.consumeResponse(ResponseBuilder::makeInt(7))); // server sent an extra response, not good
}

TEST(ConnectionHandler, IgnoredResponses) {
  ConnectionHandler handler(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = handler.stage(EncodedRequest::make("ping", "1234"), 1);

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_REPLY(fut1, 8);
}

TEST(ConnectionHandler, IgnoredResponsesWithReconnect) {
  ConnectionHandler handler(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = handler.stage(EncodedRequest::make("ping", "789"), 2);

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  handler.reconnection();

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(9)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(3)));
  ASSERT_REPLY(fut1, 3);
}

TEST(ConnectionHandler, Unavailable) {
  ConnectionHandler handler(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = handler.stage(EncodedRequest::make("ping", "789"));
  std::future<redisReplyPtr> fut2 = handler.stage(EncodedRequest::make("get", "asdf"));

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_REPLY(fut1, 7);

  ASSERT_FALSE(handler.consumeResponse(ResponseBuilder::makeErr("UNAVAILABLE something something")));
  handler.reconnection();

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(9)));
  ASSERT_REPLY(fut2, 9);

  std::future<redisReplyPtr> fut3 = handler.stage(EncodedRequest::make("get", "123"));
  ASSERT_FALSE(handler.consumeResponse(ResponseBuilder::makeErr("ERR unavailable")));

  handler.reconnection();

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeInt(3)));
  ASSERT_REPLY(fut3, 3);
}

TEST(ConnectionHandler, BadHandshakeResponse) {
  PingHandshake handshake("test test");
  ConnectionHandler handler(nullptr, &handshake,
    BackpressureStrategy::Default(), RetryStrategy::NoRetries());

  ASSERT_FALSE(handler.consumeResponse(ResponseBuilder::makeStr("adsf")));
  handler.reconnection();

  ASSERT_FALSE(handler.consumeResponse(ResponseBuilder::makeStr("chickens")));
  handler.reconnection();

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeStr("test test")));
  handler.reconnection();
}

TEST(ConnectionHandler, PubSubModeWithHandshakeNoRetries) {
  PingHandshake handshake("hi there");
  ConnectionHandler handler(nullptr, &handshake,
    BackpressureStrategy::Default(), RetryStrategy::NoRetries());

  std::future<redisReplyPtr> fut1 = handler.stage(EncodedRequest::make("asdf", "1234"));
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeStr("hi there")));

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeStr("chickens")));
  ASSERT_REPLY(fut1, "chickens");

  std::future<redisReplyPtr> fut2 = handler.stage(EncodedRequest::make("qqqq", "adsf"));

  MessageQueue mq;
  handler.enterSubscriptionMode(&mq);

  // should remain pending forever
  std::future<redisReplyPtr> fut3 = handler.stage(EncodedRequest::make("qqqq", "adsf"));
  std::future<redisReplyPtr> fut4 = handler.stage(EncodedRequest::make("qqqq", "adsf"));
  std::future<redisReplyPtr> fut5 = handler.stage(EncodedRequest::make("qqqq", "adsf"));

  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeStr("test")));
  ASSERT_REPLY(fut2, "test");

  std::vector<std::string> incoming = {"message", "random-channel", "payload-1"};
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeStringArray(incoming)));
  ASSERT_EQ(mq.size(), 1u);

  incoming = {"pmessage", "pattern-*", "random-channel-2", "payload-2"};
  ASSERT_TRUE(handler.consumeResponse(ResponseBuilder::makeStringArray(incoming)));
  ASSERT_EQ(mq.size(), 2u);

  Message* item = nullptr;
  auto it = mq.begin();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kMessage);
  ASSERT_EQ(item->getChannel(), "random-channel");
  ASSERT_EQ(item->getPayload(), "payload-1");

  it.next();
  mq.pop_front();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);
  ASSERT_EQ(mq.size(), 1u);

  ASSERT_EQ(item->getMessageType(), MessageType::kPatternMessage);
  ASSERT_EQ(item->getPattern(), "pattern-*");
  ASSERT_EQ(item->getChannel(), "random-channel-2");
  ASSERT_EQ(item->getPayload(), "payload-2");

  it.next();
  mq.pop_front();
  ASSERT_EQ(mq.size(), 0u);

  ASSERT_EQ(fut3.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_EQ(fut4.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_EQ(fut5.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
}

TEST(EndpointDecider, BasicSanity) {
  StandardErrorLogger logger;
  Members members;
  members.push_back(Endpoint("host1.cern.ch", 1234));
  members.push_back(Endpoint("host2.cern.ch", 2345));
  members.push_back(Endpoint("host3.cern.ch", 3456));

  EndpointDecider decider(&logger, members);
  ASSERT_EQ(decider.getNext(), Endpoint("host1.cern.ch", 1234));
  ASSERT_EQ(decider.getNext(), Endpoint("host2.cern.ch", 2345));

  decider.registerRedirection(Endpoint("host4.cern.ch", 9999));
  ASSERT_EQ(decider.getNext(), Endpoint("host4.cern.ch", 9999));
  ASSERT_EQ(decider.getNext(), Endpoint("host3.cern.ch", 3456));
  ASSERT_EQ(decider.getNext(), Endpoint("host1.cern.ch", 1234));
}

TEST(MultiBuilder, BasicSanity) {
  MultiBuilder builder;
  builder.emplace_back("GET", "123");
  builder.emplace_back("GET", "234");

  ASSERT_EQ(builder.size(), 2u);
  ASSERT_EQ(builder.getDeque()[0], EncodedRequest::make("GET", "123"));
  ASSERT_EQ(builder.getDeque()[1], EncodedRequest::make("GET", "234"));
}
