//------------------------------------------------------------------------------
// File: EndpointDecider.cc
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

#include "EndpointDecider.hh"

namespace qclient {

//----------------------------------------------------------------------------
// Constructor provided with cluster members as per configuration. We may
// contact different clusters when issued a redirection, however, outside of
// the original list.
//----------------------------------------------------------------------------
EndpointDecider::EndpointDecider(Logger *log, HostResolver *resolv, const Members &memb)
: logger(log), resolver(resolv), members(memb) {}

//------------------------------------------------------------------------------
// We were just notified of a redirection.
//------------------------------------------------------------------------------
void EndpointDecider::registerRedirection(const Endpoint &redir) {
  redirection = redir;
}

//------------------------------------------------------------------------------
// The event loop needs to reconnect - which endpoint should we target?
//------------------------------------------------------------------------------
Endpoint EndpointDecider::getNext() {
  if(!redirection.empty()) {
    Endpoint retval = redirection;
    redirection = {};
    QCLIENT_LOG(logger, LogLevel::kInfo, "redirecting to " << retval.toString());
    return retval;
  }

  Endpoint retval = members.getEndpoints()[nextMember];
  nextMember = (nextMember + 1) % members.size();
  QCLIENT_LOG(logger, LogLevel::kInfo, "attempting connection to " << retval.toString());
  return retval;
}

}
