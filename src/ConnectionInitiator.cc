//------------------------------------------------------------------------------
// File: ConnectionInitiator.cc
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

#include "qclient/ConnectionInitiator.hh"
#include "qclient/network/HostResolver.hh"
#include "qclient/Logger.hh"
#include "qclient/Utils.hh"
#include "qclient/Status.hh"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <sstream>

using namespace qclient;

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

inline std::string q(const std::string &str) {
  return SSTR("'" << str << "'");

}

ConnectionInitiator::ConnectionInitiator(const std::string &hostname, int port) {

  HostResolver resolver(new StandardErrorLogger());
  Status st;
  std::vector<ServiceEndpoint> endpoints = resolver.resolve(hostname, port, st);

  if(!st.ok()) {
    localerrno = st.getErrc();
    error = st.getMsg();
    return;
  }

  bool success = false;
  for(auto it = endpoints.begin(); it != endpoints.end(); it++) {
    if ((fd = socket(it->getAiFamily(), it->getAiSocktype(), it->getAiProtocol())) == -1) {
      continue;
    }

    const std::vector<char>& addr = it->getAddressBytes();

    if (::connect(fd, (const sockaddr*) addr.data(), addr.size()) == -1) {
      localerrno = errno;
      close(fd);
      fd = -1;
      continue;
    }

    // success
    success = true;
    break;
  }

  if(!success) {
    error = SSTR("Unable to connect to " << q(hostname) << ":" << port);
    fd = -1;
    return;
  }

  // clear any transient errors which might have occured while trying to connect
  localerrno = 0;

  // make socket non-blocking
  int rv = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
  if(rv != 0) {
    localerrno = errno;
    error = SSTR("Unable to make socket non-blocking");
    fd = -1;
  }
}
