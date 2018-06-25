//------------------------------------------------------------------------------
// File: Options.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>

#include "qclient/Options.hh"
#include "qclient/Handshake.hh"

using namespace qclient;

//------------------------------------------------------------------------------
// Fluent interface: Chain HMAC handshake. If password is empty, any existing
// handshake is left untouched.
//------------------------------------------------------------------------------
qclient::Options& Options::chainHmacHandshake(const std::string &password) {
  if(!password.empty()) {
    return chainHandshake(std::unique_ptr<Handshake>(new qclient::HmacAuthHandshake(password)));
  }

  return *this;
}

//------------------------------------------------------------------------------
// Fluent interface: Chain a handshake. Explicit transfer of ownership to
// this object.
//
// If given handshake is nullptr, nothing is done.
// If there's no existing handshake, the given handshake is set to be the
// top-level one.
//------------------------------------------------------------------------------
qclient::Options& Options::chainHandshake(std::unique_ptr<Handshake> chain) {
  if(!chain) {
    return *this;
  }

  if(!handshake) {
    handshake = std::move(chain);
    return *this;
  }

  handshake.reset(new qclient::HandshakeChainer(std::move(handshake), std::move(chain)));
  return *this;
}

//------------------------------------------------------------------------------
// Fluent interface: Enable transparent redirects
//------------------------------------------------------------------------------
qclient::Options& Options::withTransparentRedirects() {
  transparentRedirects = true;
  return *this;
}

//------------------------------------------------------------------------------
// Fluent interface: Disable transparent redirects
//------------------------------------------------------------------------------
qclient::Options& Options::withoutTransparentRedirects() {
  transparentRedirects = false;
  return *this;
}

//------------------------------------------------------------------------------
// Fluent interface: Setting backpressure strategy
//------------------------------------------------------------------------------
qclient::Options& Options::withBackpressureStrategy(const BackpressureStrategy& str) {
  backpressureStrategy = str;
  return *this;
}

//------------------------------------------------------------------------------
// Fluent interface: Setting retry strategy
//------------------------------------------------------------------------------
qclient::Options& Options::withRetryStrategy(const RetryStrategy& str) {
  retryStrategy = str;
  return *this;
}
