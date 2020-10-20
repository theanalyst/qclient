//------------------------------------------------------------------------------
// File: HashSubscription.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#ifndef QCLIENT_HASH_SUBSCRIPTION_HH
#define QCLIENT_HASH_SUBSCRIPTION_HH

#include "qclient/queueing/AttachableQueue.hh"

#include <memory>
#include <set>

namespace qclient {

class SharedHashSubscriber;

//------------------------------------------------------------------------------
//! SharedHashUpdate
//------------------------------------------------------------------------------
struct SharedHashUpdate {
  std::string key;
  std::string value;
};

//------------------------------------------------------------------------------
//! Listen for changes on a shared hash
//------------------------------------------------------------------------------
class SharedHashSubscription {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedHashSubscription(std::shared_ptr<SharedHashSubscriber> subscriber);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SharedHashSubscription();

  //----------------------------------------------------------------------------
  //! Detach from subscriber
  //----------------------------------------------------------------------------
  void detach();

  //----------------------------------------------------------------------------
  //! Check if currently attached to subscriber
  //----------------------------------------------------------------------------
  bool isAttached() const;

  //----------------------------------------------------------------------------
  // Get oldest entry, ie the front of the queue. Return false if the queue
  // is empty.
  //----------------------------------------------------------------------------
  bool front(SharedHashUpdate &out) const;

  //----------------------------------------------------------------------------
  // Remove the oldest entry, ie the front of the queue.
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
  using Callback = qclient::AttachableQueue<SharedHashUpdate, 50>::Callback;
  void attachCallback(const Callback &cb);

  //----------------------------------------------------------------------------
  // Detach callback, start behaving like a queue again
  //----------------------------------------------------------------------------
  void detachCallback();

private:
  friend class SharedHashSubscriber;
  friend class SharedHash;

  //----------------------------------------------------------------------------
  // Process incoming update
  //----------------------------------------------------------------------------
  void processIncoming(const SharedHashUpdate &update);

  //----------------------------------------------------------------------------
  //! Internal state
  //----------------------------------------------------------------------------
  qclient::AttachableQueue<SharedHashUpdate, 50> mQueue;
  std::shared_ptr<SharedHashSubscriber> mSubscriber;

};

//------------------------------------------------------------------------------
//! Forward incoming hash updates to subscriptions
//------------------------------------------------------------------------------
class SharedHashSubscriber {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedHashSubscriber();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SharedHashSubscriber();

  //----------------------------------------------------------------------------
  //! Feed update
  //----------------------------------------------------------------------------
  void feedUpdate(const SharedHashUpdate &update);

  //----------------------------------------------------------------------------
  //! Register subscription
  //----------------------------------------------------------------------------
  void registerSubscription(SharedHashSubscription *subscription);

  //----------------------------------------------------------------------------
  //! Unregister subscription
  //----------------------------------------------------------------------------
  void unregisterSubscription(SharedHashSubscription *subscription);

private:
  std::mutex mMutex;
  std::set<SharedHashSubscription*> mSubscriptions;
};


}

#endif

