//------------------------------------------------------------------------------
// File: ReconnectionListener.hh
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

#ifndef QCLIENT_RECONNECTION_LISTENER_HH
#define QCLIENT_RECONNECTION_LISTENER_HH

#include <string>

namespace qclient {

//------------------------------------------------------------------------------
// Derive from this object to receive notifications whenever QClient loses, or
// establishes a connection to the backend.
//------------------------------------------------------------------------------
class ReconnectionListener {
public:
  virtual ~ReconnectionListener() {}
  virtual void notifyConnectionLost(int64_t epoch, int errc, const std::string &msg) = 0;
  virtual void notifyConnectionEstablished(int64_t epoch) = 0;
};

}

#endif