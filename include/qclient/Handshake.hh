//------------------------------------------------------------------------------
// File: Handshakes.hh
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

#ifndef QCLIENT_HANDSHAKES_HH
#define QCLIENT_HANDSHAKES_HH

#include <vector>
#include <string>
#include "QCallback.hh"

namespace qclient {

using redisReplyPtr = std::shared_ptr<redisReply>;

//------------------------------------------------------------------------------
//! Class handshake - inherit from here.
//! Defines the first ever request to send to the remote host, and validates
//! the response. If response is not as expected, the connection is shut down.
//------------------------------------------------------------------------------
class Handshake
{
public:
  enum class Status {
    INVALID = 0,
    VALID_INCOMPLETE,
    VALID_COMPLETE
  };

  virtual ~Handshake() {}
  virtual std::vector<std::string> provideHandshake() = 0;
  virtual Status validateResponse(const redisReplyPtr &reply) = 0;
  virtual void restart() = 0;
};

//------------------------------------------------------------------------------
//! AuthHandshake - provide a password on connection initialization.
//------------------------------------------------------------------------------
class AuthHandshake : public Handshake {
public:
  AuthHandshake(const std::string &pw) : password(pw) { }

  virtual ~AuthHandshake() {}

  virtual std::vector<std::string> provideHandshake() override final {
    return { "AUTH", password };
  }

  virtual Status validateResponse(const redisReplyPtr &reply) override final {
    if(!reply) return Status::INVALID;
    if(reply->type != REDIS_REPLY_STATUS) return Status::INVALID;

    std::string response(reply->str, reply->len);
    if(response != "OK") return Status::INVALID;
    return Status::VALID_COMPLETE;
  }

  virtual void restart() override final {}

private:
  std::string password;
};

}

#endif
