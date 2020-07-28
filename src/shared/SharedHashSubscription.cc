//------------------------------------------------------------------------------
// File: SharedHashSubscriber.cc
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

#include "qclient/shared/SharedHashSubscription.hh"
#include "qclient/utils/Macros.hh"

#include <iostream>

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedHashSubscription::SharedHashSubscription(std::shared_ptr<SharedHashSubscriber> subscriber) {
  mSubscriber = subscriber;
  mSubscriber->registerSubscription(this);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SharedHashSubscription::~SharedHashSubscription() {
  mSubscriber->unregisterSubscription(this);
}

//------------------------------------------------------------------------------
// Check if currently attached to subscriber
//------------------------------------------------------------------------------
bool SharedHashSubscription::isAttached() const {
  return mSubscriber.get() != nullptr;
}

//------------------------------------------------------------------------------
// Detach from subscriber
//------------------------------------------------------------------------------
void SharedHashSubscription::detach() {
  mSubscriber.reset();
}

//------------------------------------------------------------------------------
// Process incoming update
//------------------------------------------------------------------------------
void SharedHashSubscription::processIncoming(const SharedHashUpdate &update) {

}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SharedHashSubscriber::~SharedHashSubscriber() {
  qclient_assert(mSubscriptions.size() == 0u);
}

//------------------------------------------------------------------------------
// Feed update
//------------------------------------------------------------------------------
void SharedHashSubscriber::feedUpdate(const SharedHashUpdate &update) {
  std::unique_lock<std::mutex> lock(mMutex);
  for(auto it = mSubscriptions.begin(); it != mSubscriptions.end(); it++) {
    (*it)->processIncoming(update);
  }
}

//------------------------------------------------------------------------------
// Register subscription
//------------------------------------------------------------------------------
void SharedHashSubscriber::registerSubscription(SharedHashSubscription *subscription) {
  std::unique_lock<std::mutex> lock(mMutex);
  mSubscriptions.insert(subscription);
}

//------------------------------------------------------------------------------
// Unregister subscription
//------------------------------------------------------------------------------
void SharedHashSubscriber::unregisterSubscription(SharedHashSubscription *subscription) {
  std::unique_lock<std::mutex> lock(mMutex);
  mSubscriptions.erase(subscription);
}



}
