// ----------------------------------------------------------------------
// File: Formatting.cc
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

#include "qclient/Reply.hh"
#include "qclient/Formatting.hh"
#include <hiredis/hiredis.h>
#include <sstream>
#include <memory>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

namespace {
std::string escapeNonPrintable(const std::string &str) {
  std::stringstream ss;

  for(size_t i = 0; i < str.size(); i++) {
    if(isprint(str[i])) {
      ss << str[i];
    }
    else if(str[i] == '\0') {
      ss << "\\x00";
    }
    else {
      char buff[16];
      snprintf(buff, 16, "\\x%02X", (unsigned char) str[i]);
      ss << buff;
    }
  }
  return ss.str();
}
}

namespace qclient {

std::string describeRedisReply(const redisReply *const redisReply, const std::string &prefix) {
  if(!redisReply) {
    return SSTR(prefix << "nullptr");
  }

  if(redisReply->type == REDIS_REPLY_NIL) {
    return SSTR(prefix << "(nil)");
  }

  if(redisReply->type == REDIS_REPLY_INTEGER) {
    return SSTR(prefix <<"(integer) " << redisReply->integer);
  }

  if(redisReply->type == REDIS_REPLY_ERROR) {
    return SSTR(prefix << "(error) " << escapeNonPrintable(std::string(redisReply->str, redisReply->len)));
  }

  if(redisReply->type == REDIS_REPLY_STATUS) {
    return SSTR(prefix << escapeNonPrintable(std::string(redisReply->str, redisReply->len)));
  }

  if(redisReply->type == REDIS_REPLY_STRING) {
    return SSTR(prefix << "\"" <<
      escapeNonPrintable(std::string(redisReply->str, redisReply->len)) <<
      "\"");
  }

  std::string spacePrefix;
  for(size_t i = 0; i < prefix.size(); i++) {
    spacePrefix += " ";
  }

  if(redisReply->type == REDIS_REPLY_ARRAY) {
    std::stringstream ss;

    if(redisReply->elements == 0u) {
      ss << prefix << "(empty list or set)" << std::endl;
    }

    for(size_t i = 0; i < redisReply->elements; i++) {
      if(i == 0) {
        ss << describeRedisReply(redisReply->element[i], SSTR(prefix << i+1 << ") ") );
      }
      else {
        ss << describeRedisReply(redisReply->element[i], SSTR(spacePrefix << i+1 << ") ") );
      }

      if(redisReply->element[i]->type != REDIS_REPLY_ARRAY) {
        ss << std::endl;
      }
    }

    return ss.str();
  }

  return SSTR(prefix << "!!! unknown reply type !!!");
}

std::string describeRedisReply(const redisReplyPtr &redisReply) {
  return describeRedisReply(redisReply.get(), "");
}

//------------------------------------------------------------------------------
// Internal API: Serialize onto a given std::ostringstream.
// We need this to support efficient serialization of arrays.
//------------------------------------------------------------------------------
void Formatting::serializeInternal(std::ostringstream &ss, const std::string &str) {
  ss << "$" << str.size() << "\r\n" << str << "\r\n";
}

void Formatting::serializeInternal(std::ostringstream &ss, int64_t num) {
  ss << ":" << num << "\r\n";
}

}
