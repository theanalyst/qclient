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

private:
  friend class SharedHashSubscriber;

  //----------------------------------------------------------------------------
  // Internal state
  //----------------------------------------------------------------------------
  qclient::AttachableQueue<SharedHashUpdate, 50> queue;
  std::shared_ptr<SharedHashSubscriber> subscriber;

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

private:
  std::set<SharedHashSubscription*> mSubscriptions;
};


}

#endif

