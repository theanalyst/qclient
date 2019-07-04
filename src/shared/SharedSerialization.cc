//------------------------------------------------------------------------------
// File: SharedSerialization.cc
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

#include "SharedSerialization.hh"
#include "qclient/Formatting.hh"
#include <sstream>
#include <sys/types.h>
#include <string.h>
#include <iostream>

#ifdef __APPLE__

#include <libkern/OSByteOrder.h>

#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

#endif

static int64_t binaryStringToInt(const char* buff) {
  int64_t result;
  memcpy(&result, buff, sizeof(result));
  return be64toh(result);
}

static void intToBinaryString(int64_t num, char* buff) {
  int64_t be = htobe64(num);
  memcpy(buff, &be, sizeof(be));
}

namespace qclient {

//------------------------------------------------------------------------------
//! Utilities for serializing the payload of messages intended for shared
//! data structures.
//------------------------------------------------------------------------------
std::string serializeBatch(const std::map<std::string, std::string> &batch) {
  std::string retval;

  size_t retvalSize = 8; // 8 bytes for array size
  for(auto it = batch.begin(); it != batch.end(); it++) {
    // 8 bytes for key size + actual key
    retvalSize += 8 + it->first.size();

    // 8 bytes for value size + actual value
    retvalSize += 8 + it->second.size();
  }

  retval.resize(retvalSize);
  char* pos = (char*) retval.data();

  intToBinaryString(batch.size() * 2, pos);
  pos += 8;

  for(auto it = batch.begin(); it != batch.end(); it++) {
    intToBinaryString(it->first.size(), pos);
    pos += 8;
    memcpy(pos, it->first.data(), it->first.size());
    pos += it->first.size();

    intToBinaryString(it->second.size(), pos);
    pos += 8;
    memcpy(pos, it->second.data(), it->second.size());
    pos += it->second.size();
  }

  return retval;
}

bool parseBatch(const std::string &payload, std::map<std::string, std::string> &out) {
  out.clear();

  if(payload.size() < 8) return false;
  int64_t elements = 0;

  elements = binaryStringToInt(payload.data());
  size_t pos = 8;

  if(elements < 0 || elements % 2 != 0) return false;

  std::string key;
  for(int64_t i = 0; i < elements; i++) {
    if(pos >= payload.size()) return false;

    // parse string length
    int64_t stringSize = binaryStringToInt(payload.data()+pos);
    pos += 8;
    if(pos+stringSize > payload.size()) return false;

    // parse string
    std::string value;
    value.resize(stringSize);
    memcpy( (char*) value.data(), payload.data()+pos, stringSize);
    pos += stringSize;

    // adjust output
    if(i % 2 != 0) {
      out[key] = value;
    }
    else {
      key = std::move(value);
    }
  }

  return true;
}


}
