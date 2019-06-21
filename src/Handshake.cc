//------------------------------------------------------------------------------
// File: Handshake.cc
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

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <iostream>
#include "qclient/Handshake.hh"
#include "qclient/utils/Macros.hh"
using namespace qclient;

//------------------------------------------------------------------------------
// AuthHandshake: Constructor
//------------------------------------------------------------------------------
AuthHandshake::AuthHandshake(const std::string &pw) : password(pw) {

}

//------------------------------------------------------------------------------
// AuthHandshake: Destructor
//------------------------------------------------------------------------------
AuthHandshake::~AuthHandshake() {}

//------------------------------------------------------------------------------
// AuthHandshake: ProvideHandshake
//------------------------------------------------------------------------------
std::vector<std::string> AuthHandshake::provideHandshake() {
  return { "AUTH", password };
}

//------------------------------------------------------------------------------
// AuthHandshake: Response validation
//------------------------------------------------------------------------------
Handshake::Status AuthHandshake::validateResponse(const redisReplyPtr &reply) {
  if(!reply) return Status::INVALID;
  if(reply->type != REDIS_REPLY_STATUS) return Status::INVALID;

  std::string response(reply->str, reply->len);
  if(response != "OK") return Status::INVALID;
  return Status::VALID_COMPLETE;
}

//------------------------------------------------------------------------------
// AuthHandshake: Restart
//------------------------------------------------------------------------------
void AuthHandshake::restart() {}

//------------------------------------------------------------------------------
// Create a new handshake object of this type
//------------------------------------------------------------------------------
std::unique_ptr<Handshake> AuthHandshake::clone() const {
  return std::unique_ptr<Handshake>(new AuthHandshake(password));
}

//------------------------------------------------------------------------------
// HmacAuthHandshake: Constructor
//------------------------------------------------------------------------------
HmacAuthHandshake::HmacAuthHandshake(const std::string &pw) : password(pw) {}

//------------------------------------------------------------------------------
// HmacAuthHandshake: Destructor
//------------------------------------------------------------------------------
HmacAuthHandshake::~HmacAuthHandshake() {}

//------------------------------------------------------------------------------
// HmacAuthHandshake: Generate cryptographically secure random bytes
//------------------------------------------------------------------------------
std::string HmacAuthHandshake::generateSecureRandomBytes(size_t nbytes) {
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

//------------------------------------------------------------------------------
// HmacAuthHandshake: Generate signature
//------------------------------------------------------------------------------
std::string HmacAuthHandshake::generateSignature() {
  std::string ret;
  ret.resize(SHA256_DIGEST_LENGTH);

  unsigned int bufferLen = SHA256_DIGEST_LENGTH;

  HMAC(EVP_sha256(), (const unsigned char*) password.c_str(), password.size(),
    (const unsigned char*) stringToSign.c_str(), stringToSign.size(), (unsigned char*) ret.data(), &bufferLen);

  return ret;
}

//------------------------------------------------------------------------------
// HmacAuthHandshake: Provide handshake
//------------------------------------------------------------------------------
std::vector<std::string> HmacAuthHandshake::provideHandshake() {
  if(initiated == false) {
    initiated = true;
    randomBytes = generateSecureRandomBytes(64);
    return { "HMAC-AUTH-GENERATE-CHALLENGE", randomBytes };
  }

  return { "HMAC-AUTH-VALIDATE-CHALLENGE", generateSignature() };
}

//------------------------------------------------------------------------------
// HmacAuthHandshake: Validate response
//------------------------------------------------------------------------------
Handshake::Status HmacAuthHandshake::validateResponse(const redisReplyPtr &reply) {
  if(!reply) return Status::INVALID;

  if(reply->type == REDIS_REPLY_ERROR) {
    std::cerr << "qclient: HmacAuthHandshake failed with error " << std::string(reply->str, reply->len) << std::endl;
    return Status::INVALID;
  }

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

//------------------------------------------------------------------------------
// HmacAuthHandshake: Restart
//------------------------------------------------------------------------------
void HmacAuthHandshake::restart() {
  initiated = false;
  receivedChallenge = false;
  randomBytes.clear();
  stringToSign.clear();
}

//------------------------------------------------------------------------------
// Create a new handshake object of this type
//------------------------------------------------------------------------------
std::unique_ptr<Handshake> HmacAuthHandshake::clone() const {
  return std::unique_ptr<Handshake>(new HmacAuthHandshake(password));
}

//------------------------------------------------------------------------------
// HandshakeChainer: Constructor
//------------------------------------------------------------------------------
HandshakeChainer::HandshakeChainer(std::unique_ptr<Handshake> h1,
  std::unique_ptr<Handshake> h2) : first(std::move(h1)), second(std::move(h2)) {}

//------------------------------------------------------------------------------
// HandshakeChainer: Destructor
//------------------------------------------------------------------------------
HandshakeChainer::~HandshakeChainer() {}

//------------------------------------------------------------------------------
// HandshakeChainer: Provide handshake
//------------------------------------------------------------------------------
std::vector<std::string> HandshakeChainer::provideHandshake() {
  if(!firstDone) {
    return first->provideHandshake();
  }

  return second->provideHandshake();
}

//------------------------------------------------------------------------------
// HandshakeChainer: Validate response
//------------------------------------------------------------------------------
Handshake::Status HandshakeChainer::validateResponse(const redisReplyPtr &reply) {
  if(!firstDone) {
    Status st = first->validateResponse(reply);
    if(st == Status::VALID_COMPLETE) {
      firstDone = true;
      return Status::VALID_INCOMPLETE;
    }

    return st;
  }

  return second->validateResponse(reply);
}

//------------------------------------------------------------------------------
// HandshakeChainer: Restart
//------------------------------------------------------------------------------
void HandshakeChainer::restart() {
  firstDone = false;
  first->restart();
  second->restart();
}

//------------------------------------------------------------------------------
// Create a new handshake object of this type
//------------------------------------------------------------------------------
std::unique_ptr<Handshake> HandshakeChainer::clone() const {
  return std::unique_ptr<Handshake>(new HandshakeChainer(first->clone(), second->clone()));
}

//------------------------------------------------------------------------------
// PingHandshake: Constructor
//------------------------------------------------------------------------------
PingHandshake::PingHandshake(const std::string &text) : pingToSend(text) {
  if(pingToSend.empty()) {
    pingToSend = "qclient-connection-initialization";
  }
}

//------------------------------------------------------------------------------
// PingHandshake: Destructor
//------------------------------------------------------------------------------
PingHandshake::~PingHandshake() {}

//------------------------------------------------------------------------------
// PingHandshake: Provide handshake
//------------------------------------------------------------------------------
std::vector<std::string> PingHandshake::provideHandshake() {
  return { "PING", pingToSend };
}

//------------------------------------------------------------------------------
// PingHandshake: Validate response
//------------------------------------------------------------------------------
Handshake::Status PingHandshake::validateResponse(const redisReplyPtr &reply) {
  if(!reply) return Status::INVALID;
  if(reply->type != REDIS_REPLY_STRING) return Status::INVALID;

  std::string response(reply->str, reply->len);
  if(response != pingToSend) return Status::INVALID;
  return Status::VALID_COMPLETE;
}

//------------------------------------------------------------------------------
// PingHandshake: Restart
//------------------------------------------------------------------------------
void PingHandshake::restart() {
}

//------------------------------------------------------------------------------
// Create a new handshake object of this type
//------------------------------------------------------------------------------
std::unique_ptr<Handshake> PingHandshake::clone() const {
  return std::unique_ptr<Handshake>(new PingHandshake(pingToSend));
}

