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

namespace qclient {

class EndpointDecider {
public:
  EndpointDecider(Logger *log, const Members &memb) : logger(log), members(memb) {}

  void registerRedirection(const Endpoint &redir) {
    redirection = redir;
  }

  Endpoint getNext() {
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

private:
  Logger *logger;
  size_t nextMember = 0u;
  Members members;
  Endpoint redirection;
};

}

#endif