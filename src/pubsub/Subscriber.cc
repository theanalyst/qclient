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

namespace qclient {

//----------------------------------------------------------------------------
// Destructor - notify subscriber we're shutting down
//----------------------------------------------------------------------------
Subscription::~Subscription() {
  if(subscriber) {
    subscriber->unsubscribe(this);
  }
}

//------------------------------------------------------------------------------
// Simulated mode - enable ability to feed fake messages for testing
// this class
//------------------------------------------------------------------------------
Subscriber::Subscriber() {

}

//------------------------------------------------------------------------------
// Receive notification about a Subscription being destroyed
//------------------------------------------------------------------------------
void Subscriber::unsubscribe(Subscription *subscription) {

}


//------------------------------------------------------------------------------
// Feed fake message - only has an effect in sumulated mode
//------------------------------------------------------------------------------
void Subscriber::feedFakeMessage(Message&& msg) {

}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void Subscriber::processIncomingMessage(Message &&msg) {
  std::lock_guard<std::mutex> lock(mtx);

}



}
