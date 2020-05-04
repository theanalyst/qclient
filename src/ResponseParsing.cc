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

IntegerParser::IntegerParser(const redisReply *reply) {
  if(reply == nullptr) {
    error = "Received null redisReply";
    isOk = false;
    return;
  }

  if(reply->type != REDIS_REPLY_INTEGER) {
    error = SSTR("Unexpected reply type; was expecting INTEGER, received " << qclient::describeRedisReply(reply));
    isOk = false;
    return;
  }

  isOk = true;
  val = reply->integer;
}

IntegerParser::IntegerParser(const redisReplyPtr reply) : IntegerParser(reply.get()) { }

bool IntegerParser::ok() const {
  return isOk;
}

std::string IntegerParser::err() const {
  return error;
}

long long IntegerParser::value() const {
  return val;
}

StringParser::StringParser(const redisReply *reply) {
  if(reply == nullptr) {
    error = "Received null redisReply";
    isOk = false;
    return;
  }

  if(reply->type != REDIS_REPLY_STRING) {
    error = SSTR("Unexpected reply type; was expecting STRING, received " << qclient::describeRedisReply(reply));
    isOk = false;
    return;
  }

  isOk = true;
  val = std::string(reply->str, reply->len);
}

StringParser::StringParser(const redisReplyPtr reply) : StringParser(reply.get()) { }

bool StringParser::ok() const {
  return isOk;
}

std::string StringParser::err() const {
  return error;
}

std::string StringParser::value() const {
  return val;
}

HgetallParser::HgetallParser(const redisReplyPtr reply) : HgetallParser(reply.get()) {}

HgetallParser::HgetallParser(const redisReply *reply) {
  if(reply == nullptr) {
    error = "Received null redisReply";
    isOk = false;
    return;
  }

  if(reply->type != REDIS_REPLY_ARRAY) {
    error = SSTR("Unexpected reply type; was expecting ARRAY, received " << qclient::describeRedisReply(reply));
    isOk = false;
    return;
  }

  if(reply->elements % 2 != 0) {
    error = SSTR("Unexpected number of elements; expected a multiple of 2, received " << reply->elements);
    isOk = false;
    return;
  }

  for(size_t i = 0; i < reply->elements; i += 2) {
    StringParser parserKey(reply->element[i]);

    if(!parserKey.ok()) {
      error = SSTR("Unexpected reply type for element #" << i << ": " << parserKey.err());
      isOk = false;
      return;
    }

    StringParser parserValue(reply->element[i+1]);
    if(!parserValue.ok()) {
      error = SSTR("Unexpected reply type for element #" << i+1 << ": " << parserValue.err());
      isOk = false;
      return;
    }

    if(val.find(parserKey.value()) != val.end()) {
      error = SSTR("Found duplicate key: '" << parserKey.value() << "'");
      isOk = false;
      return;
    }

    val.emplace(parserKey.value(), parserValue.value());
  }

  isOk = true;
}

bool HgetallParser::ok() const {
  return isOk;
}

std::string HgetallParser::err() const {
  return error;
}

std::map<std::string, std::string> HgetallParser::value() const {
  return val;
}

}
