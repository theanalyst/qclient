//------------------------------------------------------------------------------
// File: TransientSharedHash.cc
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

#include "qclient/shared/TransientSharedHash.hh"
#include "qclient/pubsub/Subscriber.hh"

namespace qclient {

//------------------------------------------------------------------------------
//! Private constructor - use SharedManager to instantiate this object.
//------------------------------------------------------------------------------
TransientSharedHash::TransientSharedHash(SharedManager *sm,
  const std::string &chan, std::unique_ptr<qclient::Subscription> sub)
: sharedManager(sm), channel(chan), subscription(std::move(sub)) {

  using namespace std::placeholders;
  subscription->attachCallback(std::bind(&TransientSharedHash::processIncoming, this, _1));
}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void TransientSharedHash::processIncoming(Message &&msg) {

}

//------------------------------------------------------------------------------
// Set key to the given value.
//------------------------------------------------------------------------------
void TransientSharedHash::set(const std::string &key, const std::string &value) {

}



}
