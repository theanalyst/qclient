//------------------------------------------------------------------------------
// File: Formatting.hh
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

#ifndef QCLIENT_FORMATTING_HH
#define QCLIENT_FORMATTING_HH

#include <string>
#include <vector>
#include <map>
#include <sstream>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

namespace qclient {

//------------------------------------------------------------------------------
// A class to help redis-serialize any given data structures
// NOTE: When in doubt, this class will serialize into string-type messages,
// not status-type messages. (status messages are not binary-safe)
//------------------------------------------------------------------------------
class Formatting {
public:

  //----------------------------------------------------------------------------
  // External API: Serialize onto a plain string.
  //----------------------------------------------------------------------------
  template<typename T>
  static std::string serialize(T&& arg) {
    std::ostringstream ss;
    serializeInternal(ss, std::forward<T>(arg));
    return ss.str();
  }

  //----------------------------------------------------------------------------
  // Serialize a vector of arbitrary types
  //----------------------------------------------------------------------------
  template<typename... Args>
  static std::string serializeVector(Args&&... args) {
    const size_t n = sizeof...(args);

    std::ostringstream ss;
    ss << "*" << n << "\r\n";
    serializeMulti(ss, std::forward<Args>(args)...);
    return ss.str();
  }

private:

  //----------------------------------------------------------------------------
  // Internal API: Serialize onto a given std::ostringstream.
  // We need this to support efficient serialization of arrays.
  //----------------------------------------------------------------------------
  static void serializeInternal(std::ostringstream &ss, const std::string &str);
  static void serializeInternal(std::ostringstream &ss, int64_t num);

  //----------------------------------------------------------------------------
  // Serialize any kind of vector
  //----------------------------------------------------------------------------
  template<typename T>
  static void serializeInternal(std::ostringstream &ss, const std::vector<T> &vec) {
    ss << "*" << vec.size() << "\r\n";
    for(size_t i = 0; i < vec.size(); i++) {
      serializeInternal(ss, vec[i]);
    }
  }

  //----------------------------------------------------------------------------
  // Serialize any kind of map
  //----------------------------------------------------------------------------
  template<typename K, typename V>
  static void serializeInternal(std::ostringstream &ss, const std::map<K, V> &map) {
    ss << "*" << 2*map.size() << "\r\n";
    for(auto it = map.begin(); it != map.end(); it++) {
      serializeInternal(ss, it->first);
      serializeInternal(ss, it->second);
    }
  }

  //----------------------------------------------------------------------------
  // Recursively serialize each type in the vector
  //----------------------------------------------------------------------------
  template<class none = void>
  static void serializeMulti(std::ostringstream &ss) {} // base case for recursion

  template<typename Head, typename... Tail>
  static void serializeMulti(std::ostringstream &ss, Head&& head, Tail&&... tail) {
    serializeInternal(ss, std::forward<Head>(head));
    serializeMulti<Tail...>(ss, std::forward<Tail>(tail)...);
  }


};

}

#endif
