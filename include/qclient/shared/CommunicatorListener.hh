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

#include <string>
#include <memory>

namespace qclient {

class Subscriber;
class Subscription;
class Message;

//------------------------------------------------------------------------------
// Convenience class to receive messages sent by Communicator.
//------------------------------------------------------------------------------
class CommunicatorListener {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  CommunicatorListener(Subscriber *subscriber, const std::string &channel);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~CommunicatorListener();

private:
  //----------------------------------------------------------------------------
  // Process incoming message
  //----------------------------------------------------------------------------
  void processIncoming(Message &&msg);

  Subscriber *mSubscriber;
  std::string mChannel;
  std::unique_ptr<Subscription> mSubscription;

};


}

#endif

