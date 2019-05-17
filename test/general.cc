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
#include "qclient/network/HostResolver.hh"
#include "qclient/pubsub/MessageQueue.hh"
#include "qclient/Status.hh"
#include "ConnectionCore.hh"
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

TEST(ResponseBuilder, MakeStatus) {
  redisReplyPtr reply = ResponseBuilder::makeStatus("aaa");
  ASSERT_NE(reply, nullptr);

  ASSERT_EQ(reply->type, REDIS_REPLY_STATUS);
  ASSERT_EQ(std::string(reply->str, reply->len), "aaa");
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

TEST(ConnectionCore, NoRetries) {
  ConnectionCore core(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::NoRetries());

  std::future<redisReplyPtr> fut1 = core.stage(EncodedRequest::make("ping", "123"));
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeErr("UNAVAILABLE test test")));

  ASSERT_REPLY(fut1, "UNAVAILABLE test test");
}

TEST(ConnectionCore, BasicSanity) {
  ConnectionCore core(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = core.stage(EncodedRequest::make("ping", "asdf1"));
  std::future<redisReplyPtr> fut2 = core.stage(EncodedRequest::make("ping", "asdf2"));
  std::future<redisReplyPtr> fut3 = core.stage(EncodedRequest::make("ping", "asdf3"));

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(5)));
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(9)));

  ASSERT_REPLY(fut1, 5);
  ASSERT_REPLY(fut2, 7);
  ASSERT_REPLY(fut3, 9);
}

TEST(ConnectionCore, Overflow) {
  ConnectionCore core(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = core.stage(EncodedRequest::make("ping", "123"));

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_FALSE(core.consumeResponse(ResponseBuilder::makeInt(7))); // server sent an extra response, not good
}

TEST(ConnectionCore, IgnoredResponses) {
  ConnectionCore core(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = core.stage(EncodedRequest::make("ping", "1234"), 1);

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_REPLY(fut1, 8);
}

TEST(ConnectionCore, IgnoredResponsesWithReconnect) {
  ConnectionCore core(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = core.stage(EncodedRequest::make("ping", "789"), 2);

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  core.reconnection();

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(8)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(9)));
  ASSERT_EQ(fut1.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(3)));
  ASSERT_REPLY(fut1, 3);
}

TEST(ConnectionCore, Unavailable) {
  ConnectionCore core(nullptr, nullptr, BackpressureStrategy::Default(), RetryStrategy::InfiniteRetries());

  std::future<redisReplyPtr> fut1 = core.stage(EncodedRequest::make("ping", "789"));
  std::future<redisReplyPtr> fut2 = core.stage(EncodedRequest::make("get", "asdf"));

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(7)));
  ASSERT_REPLY(fut1, 7);

  ASSERT_FALSE(core.consumeResponse(ResponseBuilder::makeErr("UNAVAILABLE something something")));
  core.reconnection();

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(9)));
  ASSERT_REPLY(fut2, 9);

  std::future<redisReplyPtr> fut3 = core.stage(EncodedRequest::make("get", "123"));
  ASSERT_FALSE(core.consumeResponse(ResponseBuilder::makeErr("ERR unavailable")));

  core.reconnection();

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeInt(3)));
  ASSERT_REPLY(fut3, 3);
}

TEST(ConnectionCore, BadHandshakeResponse) {
  PingHandshake handshake("test test");
  ConnectionCore core(nullptr, &handshake,
    BackpressureStrategy::Default(), RetryStrategy::NoRetries());

  ASSERT_FALSE(core.consumeResponse(ResponseBuilder::makeStr("adsf")));
  core.reconnection();

  ASSERT_FALSE(core.consumeResponse(ResponseBuilder::makeStr("chickens")));
  core.reconnection();

  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeStr("test test")));
  core.reconnection();
}

TEST(ConnectionCore, PubSubModeWithHandshakeNoRetries) {
  PingHandshake handshake("hi there");

  MessageQueue mq;
  ConnectionCore core(nullptr, &handshake,
    BackpressureStrategy::Default(), RetryStrategy::NoRetries(), &mq);

  std::future<redisReplyPtr> fut1 = core.stage(EncodedRequest::make("asdf", "1234"));
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeStr("hi there")));

  // should remain pending forever
  std::future<redisReplyPtr> fut3 = core.stage(EncodedRequest::make("qqqq", "adsf"));
  std::future<redisReplyPtr> fut4 = core.stage(EncodedRequest::make("qqqq", "adsf"));
  std::future<redisReplyPtr> fut5 = core.stage(EncodedRequest::make("qqqq", "adsf"));

  std::vector<std::string> incoming = {"message", "random-channel", "payload-1"};
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeStringArray(incoming)));
  ASSERT_EQ(mq.size(), 1u);

  incoming = {"pmessage", "pattern-*", "random-channel-2", "payload-2"};
  ASSERT_TRUE(core.consumeResponse(ResponseBuilder::makeStringArray(incoming)));
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

  HostResolver resolver(&logger);
  EndpointDecider decider(&logger, &resolver, members);
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

TEST(ServiceEndpoint, BasicSanity) {
  ServiceEndpoint ipv4(ProtocolType::kIPv4, SocketType::kStream, "192.168.1.100", 9999, "example.com");
  ASSERT_EQ(ipv4.getPort(), 9999);
  ASSERT_EQ(ipv4.getPrintableAddress(), "192.168.1.100");
  ASSERT_EQ(ipv4.getOriginalHostname(), "example.com");

  ServiceEndpoint ipv6(ProtocolType::kIPv6, SocketType::kStream, "2001:db8:85a3:8d3:1319:8a2e:370:7348", 8888, "example.com");
  ASSERT_EQ(ipv6.getPort(), 8888);
  ASSERT_EQ(ipv6.getPrintableAddress(), "2001:db8:85a3:8d3:1319:8a2e:370:7348");
  ASSERT_EQ(ipv6.getOriginalHostname(), "example.com");
}

TEST(HostResolver, BasicSanity) {
  StandardErrorLogger logger;
  HostResolver resolver(&logger);

  std::vector<ServiceEndpoint> endpoints;
  endpoints.emplace_back(ProtocolType::kIPv4, SocketType::kStream, "192.168.1.100", 4444, "1.example.com");
  endpoints.emplace_back(ProtocolType::kIPv6, SocketType::kStream, "2001:db8:85a3:8d3:1319:8a2e:370:7348", 4444, "2.example.com");
  resolver.feedFake("example.com", 4444, endpoints);

  Status st;
  ASSERT_EQ(resolver.resolve("example.com", 4444, st), endpoints);
  ASSERT_TRUE(st.ok());

  ASSERT_TRUE(resolver.resolve("3.example.com", 5555, st).empty());
  ASSERT_FALSE(st.ok());
  ASSERT_EQ(st.getErrc(), ENOENT);
}

TEST(EndpointDecider, WithHostResolution) {
  Members members;
  members.push_back("1.example.com", 3333);
  members.push_back("2.example.com", 4444);

  StandardErrorLogger logger;
  HostResolver resolver(&logger);
  EndpointDecider decider(&logger, &resolver, members);

  ServiceEndpoint ex3_1(ProtocolType::kIPv4, SocketType::kStream, "192.168.1.2", 5555, "3.example.com");
  ServiceEndpoint ex3_2(ProtocolType::kIPv4, SocketType::kStream, "192.168.1.222", 5555, "3.example.com");

  resolver.feedFake("3.example.com", 5555, { ex3_1, ex3_2 });

  // no DNS entries for 3.example.com
  ServiceEndpoint connectToNext = ServiceEndpoint(ProtocolType::kIPv4, SocketType::kStream, "127.0.0.1", 9999, "example.com");
  ASSERT_FALSE(decider.getNextEndpoint(connectToNext));

  // only 1.example.com has valid entries
  std::vector<ServiceEndpoint> endpoints;
  endpoints.emplace_back(ProtocolType::kIPv4, SocketType::kStream, "192.168.1.3", 3333, "1.example.com");
  endpoints.emplace_back(ProtocolType::kIPv6, SocketType::kStream, "2001:db8:85a3:8d3:1319:8a2e:370:7348", 3333, "1.example.com");
  resolver.feedFake("1.example.com", 3333, endpoints);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[0]);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[1]);

  // cycle back to 1.example.com, since 2.example.com has no entry
  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[0]);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[1]);

  // 2.example.com comes alive.. only an IPv4 here.
  std::vector<ServiceEndpoint> endpoints2;
  endpoints2.emplace_back(ProtocolType::kIPv4, SocketType::kStream, "192.168.1.4", 4444, "2.example.com");
  resolver.feedFake("2.example.com", 4444, endpoints2);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints2[0]);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[0]);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[1]);

  // hey, we just got a redirection to 3.example.com:5555
  decider.registerRedirection(Endpoint("3.example.com", 5555));

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, ex3_1);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, ex3_2);

  // redirection hosts have been exhausted, back to cycling
  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints2[0]);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[0]);

  ASSERT_TRUE(decider.getNextEndpoint(connectToNext));
  ASSERT_EQ(connectToNext, endpoints[1]);
}
