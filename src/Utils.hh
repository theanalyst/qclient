// ----------------------------------------------------------------------
// File: Utils.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#ifndef __QCLIENT_UTILS_H__
#define __QCLIENT_UTILS_H__

#include <limits.h>
#include "fmt/fmt/format.h"

namespace qclient {

inline bool my_strtoll(const std::string &str, int64_t &ret) {
  char *endptr = NULL;
  ret = strtoll(str.c_str(), &endptr, 10);
  if(endptr != str.c_str() + str.size() || ret == LLONG_MIN || ret == LONG_LONG_MAX) {
    return false;
  }
  return true;
}

inline std::vector<std::string> split(std::string data, std::string token) {
  std::vector<std::string> output;
  size_t pos = std::string::npos;
  do {
    pos = data.find(token);
    output.push_back(data.substr(0, pos));
    if(std::string::npos != pos) data = data.substr(pos + token.size());
  } while (std::string::npos != pos);
  return output;
}

struct RedisServer {
  std::string host;
  int port;
};

inline bool parseServer(const std::string &str, RedisServer &srv) {
  std::vector<std::string> parts = split(str, ":");

  if(parts.size() != 2) return false;

  int64_t port;
  if(!my_strtoll(parts[1], port)) return false;

  srv = RedisServer { parts[0], (int) port };
  return true;
}

//------------------------------------------------------------------------------
//! Fast convert element to string representation
//!
//! @param elem element to be converted
//!
//! @return string representation
//------------------------------------------------------------------------------
template <typename T>
static std::string stringify(const T& elem)
{
  fmt::MemoryWriter out;
  out << elem;
  return out.str();
}

}

#endif
