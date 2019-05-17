//------------------------------------------------------------------------------
// File: HostResolver.cc
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

#include "qclient/network/HostResolver.hh"
#include "qclient/GlobalInterceptor.hh"
#include "qclient/Logger.hh"
#include "qclient/Status.hh"
#include <arpa/inet.h>
#include <string.h>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

namespace qclient {

//------------------------------------------------------------------------------
// Protocol type as string
//------------------------------------------------------------------------------
std::string protocolTypeToString(ProtocolType prot) {
  switch(prot) {
    case ProtocolType::kIPv4: {
      return "IPv4";
    }
    case ProtocolType::kIPv6: {
      return "IPv6";
    }
  }

  return "unknown protocol";
}

//------------------------------------------------------------------------------
// Socket type as string
//------------------------------------------------------------------------------
std::string socketTypeToString(SocketType sock) {
  switch(sock) {
    case SocketType::kStream: {
      return "stream";
    }
    case SocketType::kDatagram: {
      return "datagram";
    }
  }

  return "unknown socket";
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ServiceEndpoint::ServiceEndpoint(ProtocolType &protocol, SocketType &socket,
  const std::vector<char> addr, const std::string &original)
: protocolType(protocol), socketType(socket), address(addr),
  originalHostname(original) { }

//------------------------------------------------------------------------------
// Get stored protocol type
//------------------------------------------------------------------------------
ProtocolType ServiceEndpoint::getProtocolType() const {
  return protocolType;
}

//------------------------------------------------------------------------------
// Get socket type
//------------------------------------------------------------------------------
SocketType ServiceEndpoint::getSocketType() const {
  return socketType;
}

//------------------------------------------------------------------------------
// Get raw address bytes (the form ::connect expects)
//------------------------------------------------------------------------------
const std::vector<char>& ServiceEndpoint::getAddressBytes() const {
  return address;
}

//----------------------------------------------------------------------------
// Get printable address (ie 127.0.0.1) that a human would expect
//----------------------------------------------------------------------------
std::string ServiceEndpoint::getPrintableAddress() const {
  char buffer[INET6_ADDRSTRLEN];

  switch(protocolType) {
    case ProtocolType::kIPv4: {
      const struct sockaddr_in* sockaddr = (const struct sockaddr_in*)(address.data());
      inet_ntop(AF_INET, &(sockaddr->sin_addr), buffer, INET6_ADDRSTRLEN);
      break;
    }
    case ProtocolType::kIPv6: {
      const struct sockaddr_in6* sockaddr = (const struct sockaddr_in6*)(address.data());
      inet_ntop(AF_INET6, &(sockaddr->sin6_addr), buffer, INET6_ADDRSTRLEN);
      break;
    }
  }

  return buffer;
}

//------------------------------------------------------------------------------
// Get service port number
//------------------------------------------------------------------------------
uint16_t ServiceEndpoint::getPort() const {

  switch(protocolType) {
    case ProtocolType::kIPv4: {
      const struct sockaddr_in* sockaddr = (const struct sockaddr_in*)(address.data());
      return htons(sockaddr->sin_port);
    }
    case ProtocolType::kIPv6: {
      const struct sockaddr_in6* sockaddr = (const struct sockaddr_in6*)(address.data());
      return ntohs(sockaddr->sin6_port);
    }
  }

  return 0; // should never happen
}

//------------------------------------------------------------------------------
// Describe as a string
//----------------------------------------------- ------------------------------
std::string ServiceEndpoint::getString() const {
  std::ostringstream ss;
  ss << "[" << getPrintableAddress() << "]" << ":" << getPort() << " ("  << protocolTypeToString(protocolType) << "," <<
    socketTypeToString(socketType) << ")";

  return ss.str();
}

//----------------------------------------------------------------------------
// Get ai_family to pass to ::connect
//----------------------------------------------------------------------------
int ServiceEndpoint::getAiFamily() const {
  switch(protocolType) {
    case ProtocolType::kIPv4: {
      return AF_INET;
    }
    case ProtocolType::kIPv6: {
      return AF_INET6;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Get ai_socktype to pass to ::socket
//------------------------------------------------------------------------------
int ServiceEndpoint::getAiSocktype() const {
  switch(socketType) {
    case SocketType::kStream: {
      return SOCK_STREAM;
    }
    case SocketType::kDatagram: {
      return SOCK_DGRAM;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Get ai_protocol to pass to ::socket
//------------------------------------------------------------------------------
int ServiceEndpoint::getAiProtocol() const {
  switch(socketType) {
    case SocketType::kStream: {
      return IPPROTO_TCP;
    }
    case SocketType::kDatagram: {
      return IPPROTO_UDP;
    }
  }

  return 0;
}

//----------------------------------------------------------------------------
// Recover original hostname, the one we passed to HostResolver
//----------------------------------------------------------------------------
std::string ServiceEndpoint::getOriginalHostname() const {
  return originalHostname;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
HostResolver::HostResolver(Logger *log) : logger(log) { }

//------------------------------------------------------------------------------
// Main resolve function: How many service endpoints match the given
// hostname and port pair?
//------------------------------------------------------------------------------
std::vector<ServiceEndpoint> HostResolver::resolve(const std::string &host, int port, Status &st) {
  std::vector<ServiceEndpoint> output;

  struct addrinfo hints, *servinfo, *p;

  int rv;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_CANONNAME;

  if ((rv = getaddrinfo(host.c_str(), std::to_string(port).c_str(),
                        &hints, &servinfo)) != 0) {
    st = Status(rv, SSTR("error when resolving '" << host << "': " << gai_strerror(rv)));
    return output;
  }

  //----------------------------------------------------------------------------
  // getaddrinfo was successful: Loop through all results, build list of
  // service endpoints
  //----------------------------------------------------------------------------
  for (p = servinfo; p != NULL; p = p->ai_next) {
    std::vector<char> addr(p->ai_addrlen);
    memcpy(addr.data(), p->ai_addr, addr.size());

    ProtocolType protocolType = ProtocolType::kIPv4;

    if(p->ai_family == AF_INET) {
      protocolType = ProtocolType::kIPv4;
    }
    else if(p->ai_family == AF_INET6) {
      protocolType = ProtocolType::kIPv6;
    }
    else {
      QCLIENT_LOG(logger, LogLevel::kWarn, "Encountered unknown network family during resolution of " << host << ":" << port << " - neither IPv4, nor IPv6!");
      continue;
    }

    SocketType socketType = SocketType::kStream;

    if(p->ai_socktype == SOCK_STREAM) {
      socketType = SocketType::kStream;
    }
    else if(p->ai_socktype == SOCK_DGRAM) {
      socketType = SocketType::kDatagram;
    }
    else {
      QCLIENT_LOG(logger, LogLevel::kWarn, "Encountered unknown socket type during resolution of " << host << ":" << port << " - neither stream, nor datagram!");
      continue;
    }

    output.emplace_back(protocolType, socketType, addr, host);
  }

  freeaddrinfo(servinfo);
  st = Status();
  return output;
}

}

