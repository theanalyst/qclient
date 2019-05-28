// ----------------------------------------------------------------------
// File: Subscriber.cc
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

#include "qclient/pubsub/Subscriber.hh"
#include "qclient/pubsub/Message.hh"
#include "qclient/pubsub/MessageListener.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Listener, static to this object
//------------------------------------------------------------------------------
class SubscriberListener : public MessageListener {
public:
  SubscriberListener(Subscriber *sub) : subscriber(sub) {}
  virtual ~SubscriberListener() {}
  virtual void handleIncomingMessage(const Message& msg) {
    subscriber->processIncomingMessage(msg);
  }

private:
  Subscriber *subscriber;
};

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Subscription::Subscription(Subscriber* sub) : subscriber(sub) {}

//------------------------------------------------------------------------------
// Destructor - notify subscriber we're shutting down
//------------------------------------------------------------------------------
Subscription::~Subscription() {
  if(subscriber) {
    subscriber->unsubscribe(this);
    subscriber = nullptr;
  }
}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void Subscription::processIncoming(const Message &msg) {
  queue.emplace_back(msg);
}

//------------------------------------------------------------------------------
// Is the queue empty?
//------------------------------------------------------------------------------
bool Subscription::empty() const {
  return queue.size() == 0;
}

//------------------------------------------------------------------------------
// Remove the oldest received message, ie the front of the queue.
//------------------------------------------------------------------------------
void Subscription::pop_front() {
  return queue.pop_front();
}

//------------------------------------------------------------------------------
// Get oldest message, ie the front of the queue. Return false if the queue
// is empty.
//------------------------------------------------------------------------------
bool Subscription::front(Message &out) const {
  if(queue.size() == 0) {
    return false;
  }

  out = queue.front();
  return true;
}

//----------------------------------------------------------------------------
// Constructor - real mode, connect to a real server
//----------------------------------------------------------------------------
Subscriber::Subscriber(const Members &members, SubscriptionOptions &&options, Logger *log)
: logger(log), listener(new SubscriberListener(this)),
  base(new BaseSubscriber(members, listener, std::move(options))) {}

//------------------------------------------------------------------------------
// Simulated mode - enable ability to feed fake messages for testing
// this class
//------------------------------------------------------------------------------
Subscriber::Subscriber() {}

//------------------------------------------------------------------------------
// Receive notification about a Subscription being destroyed
//------------------------------------------------------------------------------
void Subscriber::unsubscribe(Subscription *subscription) {
  std::lock_guard<std::mutex> lock(mtx);

  auto it = reverseChannelSubscriptions.find(subscription);
  if(it == reverseChannelSubscriptions.end()) {
    // Something is not right, warn.. TODO
    return;
  }

  channelSubscriptions.erase(it->second);
  reverseChannelSubscriptions.erase(it);
}

//------------------------------------------------------------------------------
// Feed fake message - only has an effect in sumulated mode
//------------------------------------------------------------------------------
void Subscriber::feedFakeMessage(const Message& msg) {
  processIncomingMessage(msg);
}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void Subscriber::processIncomingMessage(const Message &msg) {
  if(msg.getMessageType() != MessageType::kMessage &&
     msg.getMessageType() != MessageType::kPatternMessage) {
    return;
  }

  std::lock_guard<std::mutex> lock(mtx);

  //----------------------------------------------------------------------------
  // Feed to channel subscriptions
  //----------------------------------------------------------------------------
  auto channels = channelSubscriptions.equal_range(msg.getChannel());
  for(auto it = channels.first; it != channels.second; it++) {
    it->second->processIncoming(msg);
  }
}

//------------------------------------------------------------------------------
// Subscribe to the given channel through a Subscription object
//------------------------------------------------------------------------------
std::unique_ptr<Subscription> Subscriber::subscribe(const std::string &channel) {
  std::lock_guard<std::mutex> lock(mtx);

  std::unique_ptr<Subscription> subscription = std::make_unique<Subscription>(this);
  auto it = channelSubscriptions.emplace(channel, subscription.get());
  reverseChannelSubscriptions.emplace(subscription.get(), it);

  if(base) {
    base->subscribe( {channel} );
  }

  return subscription;
}


}
