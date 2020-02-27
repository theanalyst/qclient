//------------------------------------------------------------------------------
// File: Communicator.cc
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

#include "qclient/shared/Communicator.hh"
#include "qclient/pubsub/Subscriber.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Convenience class for point-to-point request / response messaging
//------------------------------------------------------------------------------
Communicator::Communicator(Subscriber* subscriber) : mSubscriber(subscriber),
  mQcl(mSubscriber->getQcl()) {}

//------------------------------------------------------------------------------
// Issue a request on the given channel
//------------------------------------------------------------------------------
// std::future<qclient::CommunicatorReply> Communicator::issue(const std::string &channel,
//     const std::string &contents) {
// }

}