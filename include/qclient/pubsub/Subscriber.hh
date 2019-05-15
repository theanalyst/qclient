//------------------------------------------------------------------------------
// File: Subscriber.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#ifndef QCLIENT_SUBSCRIBER_HH
#define QCLIENT_SUBSCRIBER_HH

#include "qclient/pubsub/BaseSubscriber.hh"
#include "qclient/queueing/AttachableQueue.hh"

namespace qclient {

class Subscriber;
class Message;

//------------------------------------------------------------------------------
// A pub-sub subscription which collects incoming messages. Make sure to
// consume them from time to time, or it'll blow up in space.
//
// A Subscriber must outlive all dependent Subscriptions!
//------------------------------------------------------------------------------
class Subscription {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  Subscription(Subscriber* subscriber);

  //----------------------------------------------------------------------------
  // Destructor - notify subscriber we're shutting down
  //----------------------------------------------------------------------------
  ~Subscription();

private:
  friend class Subscriber;

  //----------------------------------------------------------------------------
  // Process incoming message
  //----------------------------------------------------------------------------
  void processIncoming(const Message &msg);

  //----------------------------------------------------------------------------
  // Internal state
  //----------------------------------------------------------------------------
  qclient::AttachableQueue<Message, 100> queue;
  Subscriber *subscriber;
};


//------------------------------------------------------------------------------
// A class that builds on top of BaseSubscriber and offers a more comfortable
// API.
//------------------------------------------------------------------------------
class Subscriber {
public:
  //----------------------------------------------------------------------------
  // Constructor - real mode, connect to a real server
  //----------------------------------------------------------------------------
  Subscriber(const Members &members, SubscriptionOptions &&options);

  //----------------------------------------------------------------------------
  // Simulated mode - enable ability to feed fake messages for testing
  // this class
  //----------------------------------------------------------------------------
  Subscriber();

  //----------------------------------------------------------------------------
  // Feed fake message - only has an effect in sumulated mode
  //----------------------------------------------------------------------------
  void feedFakeMessage(Message&& msg);

  //----------------------------------------------------------------------------
  // Subscribe to the given channel through a Subscription object
  //----------------------------------------------------------------------------
  std::shared_ptr<Subscription> subscribe(const std::string &channel);

  //----------------------------------------------------------------------------
  // Subscribe to the given pattern through a Subscription object
  //----------------------------------------------------------------------------
  std::shared_ptr<Subscription> psubscribe(const std::string &pattern);


private:
  friend class Subscription;

  //----------------------------------------------------------------------------
  // Receive notification about a Subscription being destroyed
  //----------------------------------------------------------------------------
  void unsubscribe(Subscription *subscription);

  std::unique_ptr<BaseSubscriber> base;
  std::shared_ptr<MessageListener> listener;

  //----------------------------------------------------------------------------
  // Subscription maps
  //----------------------------------------------------------------------------
  std::mutex mtx;

  // std::multi map<std::string, std::shared_ptr<Subscription>> channelSubscriptions;
  // std::map<std::string, std::shared_ptr<Subscription>> patternSubscriptions;

  // std::map<Subscription*, std::string> reverseChannelSubscriptions;
  // std::map<Subscription*, std::string> reversePatternSubscriptions;

  //----------------------------------------------------------------------------
  // Process incoming message
  //----------------------------------------------------------------------------
  void processIncomingMessage(Message &&msg);




};

}

#endif
