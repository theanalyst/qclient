// ----------------------------------------------------------------------
// File: ResponseBuilder.cc
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

#include "qclient/ResponseBuilder.hh"
#include <sstream>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

namespace qclient {

ResponseBuilder::ResponseBuilder() {
  restart();
}

void ResponseBuilder::restart() {
  reader.reset(redisReaderCreate());
}

void ResponseBuilder::feed(const char* buff, size_t len) {
  if(len > 0) {
    redisReaderFeed(reader.get(), buff, len);
  }
}

void ResponseBuilder::feed(const std::string &str) {
  feed(str.c_str(), str.size());
}

ResponseBuilder::Status ResponseBuilder::pull(redisReplyPtr &out) {
  void* reply = nullptr;

  if(redisReaderGetReply(reader.get(), &reply) == REDIS_ERR) {
    return Status::kProtocolError;
  }

  if(!reply) {
    return Status::kIncomplete;
  }

  out = redisReplyPtr(redisReplyPtr((redisReply*) reply, freeReplyObject));
  return Status::kOk;
}

redisReplyPtr ResponseBuilder::makeInt(int val) {
  ResponseBuilder builder;
  builder.feed(SSTR(":" << val << "\r\n"));

  redisReplyPtr ans;
  builder.pull(ans);
  return ans;
}

redisReplyPtr ResponseBuilder::makeErr(const std::string &msg) {
  ResponseBuilder builder;
  builder.feed(SSTR("-" << msg << "\r\n"));

  redisReplyPtr ans;
  builder.pull(ans);
  return ans;
}

redisReplyPtr ResponseBuilder::makeStr(const std::string &msg) {
  ResponseBuilder builder;
  builder.feed(SSTR("$" << msg.size() << "\r\n" << msg << "\r\n"));

  redisReplyPtr ans;
  builder.pull(ans);
  return ans;
}

redisReplyPtr ResponseBuilder::makeStringArray(const std::vector<std::string> &msg) {
  ResponseBuilder builder;
  builder.feed(SSTR("*" << msg.size() << "\r\n"));

  for(size_t i = 0; i < msg.size(); i++) {
    builder.feed(SSTR("$" << msg[i].size() << "\r\n" << msg[i] << "\r\n"));
  }

  redisReplyPtr ans;
  builder.pull(ans);
  return ans;
}

redisReplyPtr ResponseBuilder::makeArr(const std::string &str1, const std::string &str2, int num) {
  ResponseBuilder builder;
  builder.feed("*3\r\n");

  builder.feed(SSTR("$" << str1.size() << "\r\n" << str1 << "\r\n"));
  builder.feed(SSTR("$" << str2.size() << "\r\n" << str2 << "\r\n"));
  builder.feed(SSTR(":" << num << "\r\n"));

  redisReplyPtr ans;
  builder.pull(ans);
  return ans;
}

}
