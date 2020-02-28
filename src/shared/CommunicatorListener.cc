//------------------------------------------------------------------------------
// File: CommunicatorListener.cc
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

#include "qclient/shared/CommunicatorListener.hh"
#include "qclient/pubsub/Subscriber.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
CommunicatorListener::CommunicatorListener(Subscriber *subscriber, const std::string &channel)
: mSubscriber(subscriber), mChannel(channel) {

  mSubscription = mSubscriber->subscribe(mChannel);

  using namespace std::placeholders;
  mSubscription->attachCallback(std::bind(&CommunicatorListener::processIncoming, this, _1));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
CommunicatorListener::~CommunicatorListener() {}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void CommunicatorListener::processIncoming(Message &&msg) {

}


}

