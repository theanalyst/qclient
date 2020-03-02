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
#include "qclient/pubsub/Message.hh"
#include "qclient/shared/PendingRequestVault.hh"
#include "shared/SharedSerialization.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
CommunicatorRequest::CommunicatorRequest(CommunicatorListener *listener, const std::string &uuid,
  const std::string &contents) : mListener(listener), mUuid(uuid), mContents(contents) {}

//------------------------------------------------------------------------------
// Get request ID
//------------------------------------------------------------------------------
std::string CommunicatorRequest::getID() const {
  return mUuid;
}

//------------------------------------------------------------------------------
// Get contents
//------------------------------------------------------------------------------
std::string CommunicatorRequest::getContents() const {
  return mContents;
}

//------------------------------------------------------------------------------
// Send reply
//------------------------------------------------------------------------------
void CommunicatorRequest::sendReply(int64_t status, const std::string &contents) {
  if(mListener) {
    mListener->sendReply(status, mUuid, contents);
  }
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
CommunicatorListener::CommunicatorListener(Subscriber *subscriber, const std::string &channel)
: mSubscriber(subscriber), mQcl(mSubscriber->getQcl()), mChannel(channel) {

  mSubscription = mSubscriber->subscribe(mChannel);

  using namespace std::placeholders;
  mSubscription->attachCallback(std::bind(&CommunicatorListener::processIncoming, this, _1));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
CommunicatorListener::~CommunicatorListener() {
  mSubscription.reset();
}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void CommunicatorListener::processIncoming(Message &&msg) {
  if(msg.getMessageType() != MessageType::kMessage) return;

  std::string uuid, contents;
  if(parseCommunicatorRequest(msg.getPayload(), uuid, contents)) {
    this->emplace_back(this, uuid, contents);
  }
}

//------------------------------------------------------------------------------
// Send reply
//------------------------------------------------------------------------------
void CommunicatorListener::sendReply(int64_t status, const std::string &uuid, const std::string &contents) {
  if(mQcl) {
    CommunicatorReply reply;
    reply.status = status;
    reply.contents = contents;

    mQcl->exec("PUBLISH", mChannel, serializeCommunicatorReply(uuid, reply));
  }
}

}

