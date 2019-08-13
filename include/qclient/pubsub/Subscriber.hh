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
class SubscriberListener;

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

  //----------------------------------------------------------------------------
  // Get oldest message, ie the front of the queue. Return false if the queue
  // is empty.
  //----------------------------------------------------------------------------
  bool front(Message &out) const;

  //----------------------------------------------------------------------------
  // Remove the oldest received message, ie the front of the queue.
  //----------------------------------------------------------------------------
  void pop_front();

  //----------------------------------------------------------------------------
  // Is the queue empty?
  //----------------------------------------------------------------------------
  bool empty() const;

  //----------------------------------------------------------------------------
  // Return queue size
  //----------------------------------------------------------------------------
  size_t size() const;

  //----------------------------------------------------------------------------
  // Stop behaving like a queue, forward incoming messages to the given
  // callback.
  //----------------------------------------------------------------------------
  using Callback = qclient::AttachableQueue<Message, 50>::Callback;
  void attachCallback(const Callback &cb);

  //----------------------------------------------------------------------------
  // Detach callback, start behaving like a queue again
  //----------------------------------------------------------------------------
  void detachCallback();

  //----------------------------------------------------------------------------
  // Has this subscription been acknowledged by the server yet?
  //----------------------------------------------------------------------------
  bool acknowledged() const;

private:
  friend class Subscriber;

  //----------------------------------------------------------------------------
  // Process incoming message
  //----------------------------------------------------------------------------
  void processIncoming(const Message &msg);

  //----------------------------------------------------------------------------
  // Mark subscription as acknowledged
  //----------------------------------------------------------------------------
  void markAcknowledged();

  //----------------------------------------------------------------------------
  // Internal state
  //----------------------------------------------------------------------------
  qclient::AttachableQueue<Message, 50> queue;
  Subscriber *subscriber;
  std::atomic<bool> isAcknowledged {false};
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
  Subscriber(const Members &members, SubscriptionOptions &&options, Logger *logger = nullptr);

  //----------------------------------------------------------------------------
  // Simulated mode - enable ability to feed fake messages for testing
  // this class
  //----------------------------------------------------------------------------
  Subscriber();

  //----------------------------------------------------------------------------
  // Feed fake message - only has an effect in sumulated mode
  //----------------------------------------------------------------------------
  void feedFakeMessage(const Message& msg);

  //----------------------------------------------------------------------------
  // Subscribe to the given channel through a Subscription object
  //----------------------------------------------------------------------------
  std::unique_ptr<Subscription> subscribe(const std::string &channel);

  //----------------------------------------------------------------------------
  // Get underlying QClient - lifetime tied to this object
  //----------------------------------------------------------------------------
  qclient::QClient* getQcl();

private:
  // Logger *logger;
  friend class Subscription;
  friend class SubscriberListener;

  //----------------------------------------------------------------------------
  // Receive notification about a Subscription being destroyed
  //----------------------------------------------------------------------------
  void unsubscribe(Subscription *subscription);

  std::shared_ptr<MessageListener> listener;
  std::unique_ptr<BaseSubscriber> base;

  //----------------------------------------------------------------------------
  // Subscription maps
  //----------------------------------------------------------------------------
  std::mutex mtx;

  std::multimap<std::string, Subscription*> channelSubscriptions;
  std::map<Subscription*, std::multimap<std::string, Subscription*>::iterator> reverseChannelSubscriptions;

  //----------------------------------------------------------------------------
  // Process incoming message
  //----------------------------------------------------------------------------
  void processIncomingMessage(const Message &msg);


};

}

#endif
