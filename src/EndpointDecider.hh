//------------------------------------------------------------------------------
// File: EndpointDecider.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef QCLIENT_ENDPOINT_DECIDER_HH
#define QCLIENT_ENDPOINT_DECIDER_HH

#include "qclient/Logger.hh"
#include "qclient/Members.hh"
#include "qclient/GlobalInterceptor.hh"
#include "qclient/network/HostResolver.hh"
#include <vector>

namespace qclient {

class HostResolver;
class ServiceEndpoint;

//------------------------------------------------------------------------------
// In face of having multiple cluster members, each cluster member entry
// potentially pointing to several IP addresses, and on top of that redirections
// which might point to hosts which are not even part of the given members...
// How do we decide where to connect to? We need to somehow take into account
// if we just received a redirection, or previous connection attempts against
// an IP failed.
//
// This class gives an answer on where to connect to next.
//------------------------------------------------------------------------------
class EndpointDecider {
public:
  //----------------------------------------------------------------------------
  // Constructor provided with cluster members as per configuration. We may
  // contact different clusters when issued a redirection, however, outside of
  // the original list.
  //----------------------------------------------------------------------------
  EndpointDecider(Logger *log, HostResolver *resolver, const Members &memb);

  //----------------------------------------------------------------------------
  // We were just notified of a redirection.
  //----------------------------------------------------------------------------
  void registerRedirection(const Endpoint &redir);

  //----------------------------------------------------------------------------
  // The event loop needs to reconnect - which endpoint should we target?
  //
  // Returns non-resolved Endpoint, leaving the DNS resolution up to the
  // caller.
  //----------------------------------------------------------------------------
  Endpoint getNext();

  //----------------------------------------------------------------------------
  // Get next fully resolved service endpoint, ready to be passed to connect().
  // False means all DNS resolution attempts failed.
  //----------------------------------------------------------------------------
  bool getNextEndpoint(ServiceEndpoint &endpoint);

  //----------------------------------------------------------------------------
  // Have we made a full circle yet? That is, have we tried all possible
  // ServiceEndpoints at least once? Including possible redirects.
  //----------------------------------------------------------------------------
  bool madeFullCircle() const;


private:
  Logger *logger;
  HostResolver *resolver;

  size_t nextMember = 0u;
  bool fullCircle = false;

  Members members;
  Endpoint redirection;

  std::vector<ServiceEndpoint> resolvedEndpoints;

  //----------------------------------------------------------------------------
  // Fetch one of the resolved endpoints, return true
  //----------------------------------------------------------------------------
  bool fetchServiceEndpoint(ServiceEndpoint &out);
};

}

#endif