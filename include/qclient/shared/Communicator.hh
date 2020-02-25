//------------------------------------------------------------------------------
// File: Communicator.hh
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

#ifndef QCLIENT_SHARED_COMMUNICATOR_HH
#define QCLIENT_SHARED_COMMUNICATOR_HH

#include <string>
#include <future>

namespace qclient {

class Subscriber;


struct CommunicatorReply {

};

//------------------------------------------------------------------------------
// Convenience class for point-to-point request / response messaging between
// two clients with QuarkDB acting as the middleman.
//
// Implements proper retries and backoff, and requires an ACK from the other
// side, which can optionally send a status code and a message.
//
// We need this for legacy reasons, if you're designing a system from scratch
// I'm not sure how reasonable doing this would be.
//
// This class is used for sending messages only. To receive them from the
// other side, use CommunicatorListener.
//------------------------------------------------------------------------------
class Communicator {
public:
  //----------------------------------------------------------------------------
  // Convenience class for point-to-point request / response messaging
  //----------------------------------------------------------------------------
  Communicator(Subscriber* subscriber);

  //----------------------------------------------------------------------------
  // Issue a request on the given channel
  //----------------------------------------------------------------------------
  std::future<CommunicatorReply> issue(const std::string &channel,
    const std::string &contents);

private:
  Subscriber* mSubscriber;

};

}


#endif
