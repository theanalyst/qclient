//------------------------------------------------------------------------------
// File: CommunicatorListener.hh
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

#ifndef QCLIENT_SHARED_COMMUNICATOR_LISTENER_HH
#define QCLIENT_SHARED_COMMUNICATOR_LISTENER_HH

#include "qclient/queueing/AttachableQueue.hh"
#include "qclient/queueing/LastNSet.hh"

#include <string>
#include <memory>

namespace qclient {

class Subscriber;
class Subscription;
class Message;
class CommunicatorListener;
class QClient;

//------------------------------------------------------------------------------
// CommunicatorRequest
//------------------------------------------------------------------------------
class CommunicatorRequest {
public:

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  CommunicatorRequest(CommunicatorListener *listener, const std::string &uuid,
    const std::string &contents);

  //----------------------------------------------------------------------------
  // Get request ID
  //----------------------------------------------------------------------------
  std::string getID() const;

  //----------------------------------------------------------------------------
  // Get contents
  //----------------------------------------------------------------------------
  std::string getContents() const;

  //----------------------------------------------------------------------------
  // Send reply
  //----------------------------------------------------------------------------
  void sendReply(int64_t status, const std::string &contents);

private:
  CommunicatorListener *mListener;
  std::string mUuid;
  std::string mContents;
};

//------------------------------------------------------------------------------
// Convenience class to receive messages sent by Communicator.
//------------------------------------------------------------------------------
class CommunicatorListener : public qclient::AttachableQueue<CommunicatorRequest, 100> {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  CommunicatorListener(Subscriber *subscriber, const std::string &channel);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~CommunicatorListener();

  //----------------------------------------------------------------------------
  // Send reply
  //----------------------------------------------------------------------------
  void sendReply(int64_t status, const std::string &uuid, const std::string &contents);

private:
  //----------------------------------------------------------------------------
  // Process incoming message
  //----------------------------------------------------------------------------
  void processIncoming(Message &&msg);

  Subscriber *mSubscriber;
  QClient *mQcl;
  std::string mChannel;
  std::unique_ptr<Subscription> mSubscription;
  LastNSet<std::string> mAlreadyReceived;
};


}

#endif

