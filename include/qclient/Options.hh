//------------------------------------------------------------------------------
// File: Options.hh
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

#ifndef QCLIENT_OPTIONS_HH
#define QCLIENT_OPTIONS_HH

namespace qclient {

//------------------------------------------------------------------------------
//! This struct specifies how to rate-limit writing into QClient.
//!
//! Since QClient offers an asynchornous API, what happens if we're able to
//! produce requests faster than they can be serviced? The request backlog size
//! will start increasing to infinity, and we'll run out of memory.
//!
//! Specifying a backpressure strategy will prevent that.
//------------------------------------------------------------------------------
class BackpressureStrategy {
public:

  //----------------------------------------------------------------------------
  //! Use this if unsure, should provide a reasonable default value.
  //----------------------------------------------------------------------------
  static BackpressureStrategy Default() {
    return RateLimitPendingRequests();
  }

  //----------------------------------------------------------------------------
  //! Limit pending requests to the specified amount. Once this limit is
  //! reached, attempts to issue more requests will block.
  //----------------------------------------------------------------------------
  static BackpressureStrategy RateLimitPendingRequests(size_t sz = 262144u) {
    BackpressureStrategy ret;
    ret.enabled = true;
    ret.pendingRequestLimit = sz;
    return ret;
  }

  //----------------------------------------------------------------------------
  //! Use this only if you have a good reason to, Default() should work fine
  //! for the vast majority of use cases.
  //----------------------------------------------------------------------------
  static BackpressureStrategy InfinitePendingRequests() {
    BackpressureStrategy ret;
    ret.enabled = false;
    return ret;
  }

  bool active() const {
    return enabled;
  }

  size_t getRequestLimit() const {
    return pendingRequestLimit;
  }

private:
  //----------------------------------------------------------------------------
  //! Private constructor - use static methods above to create an object.
  //----------------------------------------------------------------------------
  BackpressureStrategy() {}

  bool enabled = false;
  size_t pendingRequestLimit = 0u;
};

}

#endif
