// ----------------------------------------------------------------------
// File: BaseSubscriber.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "qclient/BaseSubscriber.hh"
#include "qclient/Handshake.hh"
#include "qclient/Logger.hh"

namespace qclient {


//----------------------------------------------------------------------------
// Constructor taking a list of members for the cluster
//----------------------------------------------------------------------------
BaseSubscriber::BaseSubscriber(const Members &memb,
  std::shared_ptr<MessageListener> list, SubscriptionOptions &&opt)
: members(memb), listener(list), options(std::move(opt)) {

  // Initialize a simple logger, if one was not provided
  if(!options.logger) {
    options.logger = std::make_shared<StandardErrorLogger>();
  }

  // Invalid listener?
  if(!listener) {
    QCLIENT_LOG(options.logger, LogLevel::kFatal, "Attempted to initialize qclient::BaseSubscriber object with nullptr message listener!");
    std::abort();
  }


}


}