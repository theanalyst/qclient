//------------------------------------------------------------------------------
// File: SharedQueue.cc
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

#include "qclient/shared/SharedDeque.hh"
#include "qclient/shared/SharedManager.hh"
#include "qclient/ResponseParsing.hh"
#include "qclient/QClient.hh"
#include "qclient/pubsub/Subscriber.hh"


namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedDeque::SharedDeque(SharedManager *sm, const std::string &key)
: mSharedManager(sm), mKey(key), mQcl(sm->getQClient()) {

  mSubscription = sm->getSubscriber()->subscribe(mKey);

  using namespace std::placeholders;
  mSubscription->attachCallback(std::bind(&SharedDeque::processIncoming, this, _1));

}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SharedDeque::~SharedDeque() {}

//------------------------------------------------------------------------------
// Push an element into the back of the deque
//------------------------------------------------------------------------------
void SharedDeque::push_back(const std::string &contents) {
  invalidateCachedSize();
  mSharedManager->publish(mKey, "push-back-prepare");
  mQcl->exec("deque-push-back", mKey, contents);
  mSharedManager->publish(mKey, "push-back-done");
}

//------------------------------------------------------------------------------
// Clear deque contents
//------------------------------------------------------------------------------
void SharedDeque::clear() {
  invalidateCachedSize();
  mSharedManager->publish(mKey, "clear-prepare");
  mQcl->exec("deque-clear", mKey);
  mSharedManager->publish(mKey, "clear-done");
}

//------------------------------------------------------------------------------
// Remove item from the front of the queue. If queue is empty, "" will be
// returned - not an error.
//------------------------------------------------------------------------------
qclient::Status SharedDeque::pop_front(std::string &out) {
  invalidateCachedSize();
  mSharedManager->publish(mKey, "pop-front-prepare");
  StringParser parser(mQcl->exec("deque-pop-front", mKey).get());
  mSharedManager->publish(mKey, "pop-front-done");

  if(!parser.ok()) {
    return qclient::Status(EINVAL, parser.err());
  }

  out = parser.value();
  return qclient::Status();
}

//------------------------------------------------------------------------------
//! Query deque size
//------------------------------------------------------------------------------
qclient::Status SharedDeque::size(size_t &out) {
  std::unique_lock<std::mutex> lock(mCacheMutex);

  if(mCachedSizeValid) {
    out = mCachedSize;
    return qclient::Status();
  }

  lock.unlock();

  IntegerParser parser(mQcl->exec("deque-len", mKey).get());
  if(!parser.ok()) {
    return qclient::Status(EINVAL, parser.err());
  }

  lock.lock();

  out = parser.value();
  mCachedSize = out;
  mCachedSizeValid = true;
  return qclient::Status();
}

//----------------------------------------------------------------------------
//! Invalidate cached size
//----------------------------------------------------------------------------
void SharedDeque::invalidateCachedSize() {
  std::unique_lock<std::mutex> lock(mCacheMutex);
  mCachedSize = 0u;
  mCachedSizeValid = false;
}

//------------------------------------------------------------------------------
//! Process incoming message
//------------------------------------------------------------------------------
void SharedDeque::processIncoming(Message &&msg) {
  invalidateCachedSize();
}

}
