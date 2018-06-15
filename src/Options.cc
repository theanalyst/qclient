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
//! Fluent interface: Set HMAC handshake. If password is empty, any existing
//! handshake is cleared out
//------------------------------------------------------------------------------
qclient::Options& Options::withHmacHandshake(const std::string &password) {
  handshake.reset();
  if(!password.empty()) {
    handshake.reset(new qclient::HmacAuthHandshake(password));
  }

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
