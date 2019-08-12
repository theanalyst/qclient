// ----------------------------------------------------------------------
// File: pubsub.cc
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

#include "pubsub/MessageParser.hh"
#include "qclient/ResponseBuilder.hh"
#include "qclient/pubsub/Message.hh"
#include "qclient/pubsub/MessageQueue.hh"
#include "qclient/pubsub/Subscriber.hh"
#include "gtest/gtest.h"

using namespace qclient;

TEST(MessageParser, ParseFailure) {
  Message msg;
  ASSERT_FALSE(MessageParser::parse(ResponseBuilder::makeStr("adfaf"), msg));
  ASSERT_FALSE(MessageParser::parse(ResponseBuilder::makeInt(3), msg));
}

TEST(MessageParser, kMessage) {
  Message msg;
  std::vector<std::string> vec = { "message", "mychannel", "test" };
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makeStringArray(vec), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kMessage);
  ASSERT_EQ(msg.getChannel(), "mychannel");
  ASSERT_EQ(msg.getPayload(), "test");
}


TEST(MessageParser, kMessagePush) {
  Message msg;
  std::vector<std::string> vec = { "pubsub", "message", "mychannel", "test" };
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makePushArray(vec), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kMessage);
  ASSERT_EQ(msg.getChannel(), "mychannel");
  ASSERT_EQ(msg.getPayload(), "test");
}

TEST(MessageParser, kPatternMessage) {
  Message msg;
  std::vector<std::string> vec = { "pmessage", "pattern*", "channel-name", "aaa" };
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makeStringArray(vec), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kPatternMessage);
  ASSERT_EQ(msg.getPattern(), "pattern*");
  ASSERT_EQ(msg.getChannel(), "channel-name");
  ASSERT_EQ(msg.getPayload(), "aaa");
}

TEST(MessageParser, kPatternMessagePush) {
  Message msg;
  std::vector<std::string> vec = { "pubsub", "pmessage", "pattern*", "channel-name", "aaa" };
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makePushArray(vec), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kPatternMessage);
  ASSERT_EQ(msg.getPattern(), "pattern*");
  ASSERT_EQ(msg.getChannel(), "channel-name");
  ASSERT_EQ(msg.getPayload(), "aaa");
}

TEST(MessageParser, kSubscribe) {
  Message msg;
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makeArr("subscribe", "chan", 4), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kSubscribe);
  ASSERT_EQ(msg.getChannel(), "chan");
  ASSERT_EQ(msg.getActiveSubscriptions(), 4);
}

TEST(MessageParser, kSubscribePush) {
  Message msg;
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makePushArr("pubsub", "subscribe", "chan", 4), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kSubscribe);
  ASSERT_EQ(msg.getChannel(), "chan");
  ASSERT_EQ(msg.getActiveSubscriptions(), 4);
}

TEST(MessageParser, kPatternSubscribe) {
  Message msg;
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makeArr("psubscribe", "chan2", 3), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kPatternSubscribe);
  ASSERT_EQ(msg.getPattern(), "chan2");
  ASSERT_EQ(msg.getActiveSubscriptions(), 3);
}

TEST(MessageParser, kPatternSubscribePush) {
  Message msg;
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makePushArr("pubsub", "psubscribe", "chan2", 3), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kPatternSubscribe);
  ASSERT_EQ(msg.getPattern(), "chan2");
  ASSERT_EQ(msg.getActiveSubscriptions(), 3);
}

TEST(MessageParser, kUnsubscribe) {
  Message msg;
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makeArr("unsubscribe", "mychan", 99), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kUnsubscribe);
  ASSERT_EQ(msg.getChannel(), "mychan");
  ASSERT_EQ(msg.getActiveSubscriptions(), 99);
}

TEST(MessageParser, kUnsubscribePush) {
  Message msg;
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makePushArr("pubsub", "unsubscribe", "mychan", 99), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kUnsubscribe);
  ASSERT_EQ(msg.getChannel(), "mychan");
  ASSERT_EQ(msg.getActiveSubscriptions(), 99);
}

TEST(MessageParser, kPatternUnsubscribe) {
  Message msg;
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makeArr("punsubscribe", "p*", 9999), msg));
  ASSERT_EQ(msg.getMessageType(), MessageType::kPatternUnsubscribe);
  ASSERT_EQ(msg.getPattern(), "p*");
  ASSERT_EQ(msg.getActiveSubscriptions(), 9999);
}

TEST(MessageQueue, BasicSanity) {
  MessageQueue queue;

  Message msg;
  std::vector<std::string> vec = { "message", "mychannel", "test" };
  ASSERT_TRUE(MessageParser::parse(ResponseBuilder::makeStringArray(vec), msg));

  queue.handleIncomingMessage(std::move(msg));
  ASSERT_EQ(queue.size(), 1u);

  auto it = queue.begin();
  ASSERT_TRUE(it.itemHasArrived());
  ASSERT_EQ(it.item().getMessageType(), MessageType::kMessage);
  ASSERT_EQ(it.item().getChannel(), "mychannel");

  it.next();
  queue.pop_front();

  ASSERT_EQ(queue.size(), 0u);
}

TEST(Subscriber, BasicSanity) {
  Subscriber subscriber;

  std::unique_ptr<Subscription> ch1 = subscriber.subscribe("ch1");
  ASSERT_TRUE(ch1->empty());

  subscriber.feedFakeMessage(Message::createMessage("ch2", "test"));
  ASSERT_TRUE(ch1->empty());

  subscriber.feedFakeMessage(Message::createMessage("ch1", "aaaa"));

  Message expected = Message::createMessage("ch1", "aaaa");

  Message msg;
  ASSERT_TRUE(ch1->front(msg));
  ASSERT_EQ(msg, expected);
  ch1->pop_front();
  ASSERT_TRUE(ch1->empty());

  std::unique_ptr<Subscription> ch1clone = subscriber.subscribe("ch1");
  subscriber.feedFakeMessage(Message::createMessage("ch1", "aaaa"));

  ASSERT_TRUE(ch1->front(msg));
  ASSERT_EQ(msg, expected);
  ch1->pop_front();
  ASSERT_TRUE(ch1->empty());

  ASSERT_TRUE(ch1clone->front(msg));
  ASSERT_EQ(msg, expected);
  ch1clone->pop_front();
  ASSERT_TRUE(ch1clone->empty());
}

