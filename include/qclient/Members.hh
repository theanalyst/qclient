//------------------------------------------------------------------------------
// File: Members.hh
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

#ifndef __QCLIENT_MEMBERS_H__
#define __QCLIENT_MEMBERS_H__

#include <sstream>

namespace qclient {

class Endpoint {
public:
  Endpoint(const std::string &host_, int port_) : host(host_), port(port_) {}
  Endpoint() {}

  const std::string& getHost() const {
    return host;
  }

  int getPort() const {
    return port;
  }

  bool empty() const {
    return host.empty() || port <= 0;
  }

  std::string toString() const {
    std::stringstream ss;
    ss << host << ":" << port << std::endl;
    return ss.str();
  }

private:
  std::string host;
  int port = -1;
};

class Members {
public:
  Members() {}

  Members(const std::string &host, int port) {
    members.emplace_back(host, port);
  }

  Members(const std::vector<Endpoint> &members_) : members(members_) {}

  const std::vector<Endpoint>& getEndpoints() const {
    return members;
  }

  void push_back(const std::string &host, int port) {
    members.emplace_back(host, port);
  }

  void push_back(const Endpoint& endpoint) {
    members.push_back(endpoint);
  }

  size_t size() const {
    return members.size();
  }

  bool parse(const std::string& input) {
    bool valid = false;
    std::istringstream iss(input);
    std::string token;

    while (std::getline(iss, token, ' ')) {
      size_t pos = token.find(':');

      if (pos != std::string::npos) {
        try {
          std::string host = token.substr(0, pos);
          uint32_t port = std::stoul(token.substr(pos + 1));
          members.emplace_back(host, port);
          valid = true;
        } catch (const std::exception& e) {
          continue;
        }
      }
    }

    return valid;
  }

private:
  std::vector<Endpoint> members;
};

}

#endif
