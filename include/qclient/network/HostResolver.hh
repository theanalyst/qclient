//------------------------------------------------------------------------------
// File: HostResolver.hh
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

#ifndef QCLIENT_HOST_RESOLVER_HH
#define QCLIENT_HOST_RESOLVER_HH

#include <netdb.h>
#include <vector>
#include <string>
#include <mutex>
#include <map>

namespace qclient {

class Logger;
class Status;

//------------------------------------------------------------------------------
// Protocol type
//------------------------------------------------------------------------------
enum class ProtocolType {
  kIPv4,
  kIPv6,
};

//------------------------------------------------------------------------------
// Protocol type as string
//------------------------------------------------------------------------------
std::string protocolTypeToString(ProtocolType prot);

//------------------------------------------------------------------------------
// Socket type
//------------------------------------------------------------------------------
enum class SocketType {
  kStream,
  kDatagram
};

//------------------------------------------------------------------------------
// Socket type as string
//------------------------------------------------------------------------------
std::string socketTypeToString(SocketType sock);

//------------------------------------------------------------------------------
// This class contains the necessary information to connect() directly to a
// service, no further DNS lookups should be necessary.
//------------------------------------------------------------------------------
class ServiceEndpoint {
public:

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ServiceEndpoint(ProtocolType protocol, SocketType socket,
    const std::vector<char> addr, const std::string &original);

  //----------------------------------------------------------------------------
  // Constructor, taking the IP address as text and a port, not sockaddr bytes
  //----------------------------------------------------------------------------
  ServiceEndpoint(ProtocolType protocol, SocketType socket,
    const std::string &addr, int port, const std::string &original);


  //----------------------------------------------------------------------------
  // Get stored protocol type
  //----------------------------------------------------------------------------
  ProtocolType getProtocolType() const;

  //----------------------------------------------------------------------------
  // Get socket type
  //----------------------------------------------------------------------------
  SocketType getSocketType() const;

  //----------------------------------------------------------------------------
  // Get raw address bytes (the form ::connect expects)
  //----------------------------------------------------------------------------
  const std::vector<char>& getAddressBytes() const;

  //----------------------------------------------------------------------------
  // Get printable address (ie 127.0.0.1) that a human would expect
  //----------------------------------------------------------------------------
  std::string getPrintableAddress() const;

  //----------------------------------------------------------------------------
  // Get service port number
  //----------------------------------------------------------------------------
  uint16_t getPort() const;

  //----------------------------------------------------------------------------
  // Describe as a string
  //----------------------------------------------------------------------------
  std::string getString() const;

  //----------------------------------------------------------------------------
  // Get ai_family to pass to ::socket
  //----------------------------------------------------------------------------
  int getAiFamily() const;

  //----------------------------------------------------------------------------
  // Get ai_socktype to pass to ::socket
  //----------------------------------------------------------------------------
  int getAiSocktype() const;

  //----------------------------------------------------------------------------
  // Get ai_protocol to pass to ::socket
  //----------------------------------------------------------------------------
  int getAiProtocol() const;

  //----------------------------------------------------------------------------
  // Recover original hostname, the one we passed to HostResolver
  //----------------------------------------------------------------------------
  std::string getOriginalHostname() const;

  //----------------------------------------------------------------------------
  // Equality operator
  //----------------------------------------------------------------------------
  bool operator==(const ServiceEndpoint& other) const;


private:
  ProtocolType protocolType;
  SocketType socketType;
  std::vector<char> address; // struct sockaddr bytes stored in a char vector
  std::string originalHostname;
};

//------------------------------------------------------------------------------
// Class for resolving hostnames into IPs we can directly connect to.
//------------------------------------------------------------------------------
class HostResolver {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  HostResolver(Logger *logger);

  //----------------------------------------------------------------------------
  // Main resolve function: How many service endpoints match the given
  // hostname and port pair?
  //----------------------------------------------------------------------------
  std::vector<ServiceEndpoint> resolve(const std::string &host, int port,
    Status &st);

  //----------------------------------------------------------------------------
  // Like above, but do not take global interceptions into account.
  //----------------------------------------------------------------------------
  std::vector<ServiceEndpoint> resolveNoIntercept(const std::string &host, int port,
    Status &st);


  //----------------------------------------------------------------------------
  // Feed fake data - once you call this, _all_ responses will be faked
  //----------------------------------------------------------------------------
  void feedFake(const std::string &host, int port, const std::vector<ServiceEndpoint> &out);

private:
  Logger *logger;

  std::mutex mtx;
  std::map<std::pair<std::string, int>, std::vector<ServiceEndpoint>> fakeMap;

  //----------------------------------------------------------------------------
  // Resolve function that only returns fake data
  //----------------------------------------------------------------------------
  std::vector<ServiceEndpoint> resolveFake(const std::string &host, int port,
    Status &st);

};


}

#endif
