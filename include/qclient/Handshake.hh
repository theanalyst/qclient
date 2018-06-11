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
#include "Assert.hh"
#include "Utils.hh"

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

//------------------------------------------------------------------------------
//! HmacAuthHandshake - solve an HMAC challenge in order to authenticate.
//------------------------------------------------------------------------------
class HmacAuthHandshake : public Handshake {
public:
  HmacAuthHandshake(const std::string &pw) : password(pw) {}

  virtual ~HmacAuthHandshake() {}

  static std::string generateSecureRandomBytes(size_t nbytes) {
    char buffer[nbytes + 1];

    // We might want to keep a pool of open "/dev/urandom" on standby, to avoid
    // opening and closing /dev/urandom too often, but meh, this'll do for now.

    FILE *in = fopen("/dev/urandom", "rb");

    if(!in) {
      std::cerr << "unable to open /dev/urandom" << std::endl;
      std::terminate();
    }

    size_t bytes_read = fread(buffer, 1, nbytes, in);

    if(bytes_read != nbytes) {
      std::cerr << "qclient: assertion violation, bytes_read != nbytes. " << std::endl;
      std::terminate();
    }

    qclient_assert(bytes_read == nbytes);
    qclient_assert(fclose(in) == 0);

    return std::string(buffer, nbytes);
  }

  std::string generateSignature() {
    std::string ret;
    ret.resize(SHA256_DIGEST_LENGTH);

    unsigned int bufferLen = SHA256_DIGEST_LENGTH;

    HMAC(EVP_sha256(), (const unsigned char*) password.c_str(), password.size(),
      (const unsigned char*) stringToSign.c_str(), stringToSign.size(), (unsigned char*) ret.data(), &bufferLen);

    return ret;
  }

  virtual std::vector<std::string> provideHandshake() override final {
    if(initiated == false) {
      initiated = true;
      randomBytes = generateSecureRandomBytes(64);
      return { "HMAC-AUTH-GENERATE-CHALLENGE", randomBytes };
    }

    return { "HMAC-AUTH-VALIDATE-CHALLENGE", generateSignature() };
  }

  virtual Status validateResponse(const redisReplyPtr &reply) override final {
    if(!reply) return Status::INVALID;

    if(!receivedChallenge) {
      if(reply->type != REDIS_REPLY_STRING) {
        std::cerr << "qclient: Received invalid response type in HmacAuthHandshake" << std::endl;
        return Status::INVALID;
      }

      stringToSign = std::string(reply->str, reply->len);
      receivedChallenge = true;
      if(!startswith(stringToSign, randomBytes)) {
        std::cerr << "qclient: HmacAuthHandshake: My random bytes were not used by the server for the construction of string-to-sign" << std::endl;
        return Status::INVALID;
      }

      return Status::VALID_INCOMPLETE;
    }

    if(reply->type != REDIS_REPLY_STATUS) {
      std::cerr << "qclient: Received invalid response type in HmacAuthHandshake" << std::endl;
      return Status::INVALID;
    }

    if(std::string(reply->str, reply->len) != "OK") {
      std::cerr << "qclient: HmacAuthHandshake received invalid response - " << std::string(reply->str, reply->len) << std::endl;
      return Status::INVALID;
    }

    return Status::VALID_COMPLETE;
  }

  virtual void restart() override final {
    initiated = false;
    receivedChallenge = false;
    randomBytes.clear();
    stringToSign.clear();
  }

private:
  bool initiated = false;
  bool receivedChallenge = false;
  std::string password;
  std::string randomBytes;
  std::string stringToSign;
};

}

#endif
