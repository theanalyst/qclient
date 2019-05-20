//------------------------------------------------------------------------------
// File: ConnectionInitiator.hh
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

#ifndef QCLIENT_CONNECTION_INITIATOR_HH
#define QCLIENT_CONNECTION_INITIATOR_HH

#include <string>

namespace qclient {

class ServiceEndpoint;

// Initiates a TCP connection to the specified host+port. After a successful
// connection, it does *not* manage the lifetime of the file descriptor.
class ConnectionInitiator {
public:
  //----------------------------------------------------------------------------
  // Connect to an already resolved endpoint, no DNS lookups
  //----------------------------------------------------------------------------
  ConnectionInitiator(const ServiceEndpoint &endpoint);

  //----------------------------------------------------------------------------
  // Connect to a host:port combination, resolve hostname manually.
  //----------------------------------------------------------------------------
  ConnectionInitiator(const std::string &hostname, int port);

  bool ok() {
    return (fd > 0) && (localerrno == 0) && (error.empty());
  }

  int getFd() {
    return fd;
  }

  int getErrno() {
    return localerrno;
  }

  std::string getError() {
    return error;
  }

private:
  int fd = -1;
  int localerrno = 0;
  std::string error;
};

}

#endif
