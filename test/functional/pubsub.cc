//------------------------------------------------------------------------------
// File: pubsub.cc
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
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

#include "test-config.hh"
#include "qclient/BaseSubscriber.hh"
#include "qclient/pubsub/MessageQueue.hh"
#include "qclient/Debug.hh"
#include "../ReplyMacros.hh"
#include <gtest/gtest.h>

using namespace qclient;

TEST(BaseSubscriber, BasicSanity) {
  std::shared_ptr<MessageQueue> listener { new MessageQueue() };

  Members members(testconfig.host, testconfig.port);
  BaseSubscriber subscriber(members, listener, {} );

  ASSERT_EQ(listener->size(), 0u);
  subscriber.subscribe( {"pickles"} );

  listener->setBlockingMode(true);
  Message* item = nullptr;

  auto it = listener->begin();
  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);
  ASSERT_EQ(listener->size(), 1u);

  ASSERT_EQ(item->getMessageType(), MessageType::kSubscribe);
  ASSERT_EQ(item->getChannel(), "pickles");
  ASSERT_EQ(item->getActiveSubscriptions(), 1);

  it.next();
  listener->pop_front();

  // TODO: uncomment the code below
  ASSERT_EQ(listener->size(), 0u);
  subscriber.subscribe( {"test-2"} );

  item = it.getItemBlockOrNull();
  ASSERT_EQ(listener->size(), 1u);
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kSubscribe);
  ASSERT_EQ(item->getChannel(), "test-2");
  ASSERT_EQ(item->getActiveSubscriptions(), 2);

  it.next();
  listener->pop_front();

  QClient publisher(members, {} );
  ASSERT_REPLY(publisher.exec("publish", "pickles", "penguins"), 1);

  item = it.getItemBlockOrNull();
  ASSERT_EQ(listener->size(), 1u);
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kMessage);
  ASSERT_EQ(item->getChannel(), "pickles");
  ASSERT_EQ(item->getPayload(), "penguins");

  it.next();
  listener->pop_front();

  subscriber.psubscribe( {"test-*"} );

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kPatternSubscribe);
  ASSERT_EQ(item->getPattern(), "test-*");
  ASSERT_EQ(item->getActiveSubscriptions(), 3);

  ASSERT_REPLY(publisher.exec("publish", "test-3", "asdf"), 1);

  it.next();
  listener->pop_front();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kPatternMessage);
  ASSERT_EQ(item->getPattern(), "test-*");
  ASSERT_EQ(item->getChannel(), "test-3");
  ASSERT_EQ(item->getPayload(), "asdf");

  it.next();
  listener->pop_front();

  ASSERT_REPLY(publisher.exec("publish", "test-2", "chickens"), 2);

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kMessage);
  ASSERT_EQ(item->getChannel(), "test-2");
  ASSERT_EQ(item->getPayload(), "chickens");

  it.next();
  listener->pop_front();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kPatternMessage);
  ASSERT_EQ(item->getChannel(), "test-2");
  ASSERT_EQ(item->getPattern(), "test-*");
  ASSERT_EQ(item->getPayload(), "chickens");

  it.next();
  listener->pop_front();

  subscriber.unsubscribe( {"pickles"} );

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kUnsubscribe);
  ASSERT_EQ(item->getChannel(), "pickles");
  ASSERT_EQ(item->getActiveSubscriptions(), 2);

  subscriber.unsubscribe( {"penguins"} );

  it.next();
  listener->pop_front();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kUnsubscribe);
  ASSERT_EQ(item->getChannel(), "penguins");
  ASSERT_EQ(item->getActiveSubscriptions(), 2);

  ASSERT_REPLY(publisher.exec("publish", "pickles", "no-listeners"), 0);

  subscriber.unsubscribe( {"test-2"} );

  it.next();
  listener->pop_front();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kUnsubscribe);
  ASSERT_EQ(item->getChannel(), "test-2");
  ASSERT_EQ(item->getActiveSubscriptions(), 1);

  ASSERT_REPLY(publisher.exec("publish", "test-9", "giraffes"), 1);

  it.next();
  listener->pop_front();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kPatternMessage);
  ASSERT_EQ(item->getPattern(), "test-*");
  ASSERT_EQ(item->getChannel(), "test-9");
  ASSERT_EQ(item->getPayload(), "giraffes");

  subscriber.punsubscribe( {"test-*"} );

  it.next();
  listener->pop_front();

  item = it.getItemBlockOrNull();
  ASSERT_NE(item, nullptr);

  ASSERT_EQ(item->getMessageType(), MessageType::kPatternUnsubscribe);
  ASSERT_EQ(item->getPattern(), "test-*");
  ASSERT_EQ(item->getActiveSubscriptions(), 0);
}
