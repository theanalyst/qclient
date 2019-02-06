// ----------------------------------------------------------------------
// File: ResponseParsing.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "qclient/ResponseParsing.hh"
#include "qclient/QClient.hh"
#include <sstream>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

namespace qclient {

StatusParser::StatusParser(const redisReply *reply) {
  if(reply == nullptr) {
    error = "Received null redisReply";
    isOk = false;
    return;
  }

  if(reply->type != REDIS_REPLY_STATUS) {
    error = SSTR("Unexpected reply type; was expecting STATUS, received " << qclient::describeRedisReply(reply));
    isOk = false;
    return;
  }

  isOk = true;
  val = std::string(reply->str, reply->len);
}

StatusParser::StatusParser(const redisReplyPtr reply) : StatusParser(reply.get()) { }

bool StatusParser::ok() const {
  return isOk;
}

std::string StatusParser::err() const {
  return error;
}

std::string StatusParser::value() const {
  return val;
}

}
