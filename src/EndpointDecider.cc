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
#include "qclient/Status.hh"
#include "qclient/network/HostResolver.hh"
#include "qclient/Logger.hh"
#include <algorithm>

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
  resolvedEndpoints.clear();
  redirection = redir;
}

//------------------------------------------------------------------------------
// The event loop needs to reconnect - which endpoint should we target?
//------------------------------------------------------------------------------
Endpoint EndpointDecider::getNext() {
  resolvedEndpoints.clear();

  if(!redirection.empty()) {
    Endpoint retval = redirection;
    redirection = {};
    QCLIENT_LOG(logger, LogLevel::kInfo, "Received redirection to " << retval.toString());
    return retval;
  }

  Endpoint retval = members.getEndpoints()[nextMember];
  nextMember = (nextMember + 1) % members.size();
  return retval;
}

//------------------------------------------------------------------------------
// Fetch one of the resolved endpoints, return true
//------------------------------------------------------------------------------
bool EndpointDecider::fetchServiceEndpoint(ServiceEndpoint &out) {
  out = resolvedEndpoints.back();
  resolvedEndpoints.pop_back();
  return true;
}

//------------------------------------------------------------------------------
// Get next service endpoint. False means all DNS resolution attempts failed.
//------------------------------------------------------------------------------
bool EndpointDecider::getNextEndpoint(ServiceEndpoint &resolved) {

  if(resolvedEndpoints.size() == 1 && nextMember == 0) {
    fullCircle = true;
  }

  if(!resolvedEndpoints.empty()) {
    return fetchServiceEndpoint(resolved);
  }

  for(size_t attempt = 0; attempt < members.size() + (!redirection.empty()); attempt++) {
    Endpoint endpoint = getNext();

    Status st;
    resolvedEndpoints = resolver->resolve(endpoint.getHost(), endpoint.getPort(), st);
    std::reverse(resolvedEndpoints.begin(), resolvedEndpoints.end());

    if(!st.ok() || resolvedEndpoints.empty()) {
      QCLIENT_LOG(logger, LogLevel::kWarn, "DNS resolution of " << endpoint.toString() << " failed: " << st.toString());
    }

    if(resolvedEndpoints.size() == 1 && nextMember == 0) {
      fullCircle = true;
    }

    if(!resolvedEndpoints.empty()) {
      return fetchServiceEndpoint(resolved);
    }
  }

  //----------------------------------------------------------------------------
  // All resolution requests for all members we know about failed!
  //----------------------------------------------------------------------------
  fullCircle = true;
  QCLIENT_LOG(logger, LogLevel::kError, "Unable to resolve any endpoints, possible trouble with DNS");
  return false;
}

//------------------------------------------------------------------------------
// Have we made a full circle yet? That is, have we tried all possible
// ServiceEndpoints at least once? Including possible redirects.
//------------------------------------------------------------------------------
bool EndpointDecider::madeFullCircle() const {
  return fullCircle;
}


}
