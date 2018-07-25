//------------------------------------------------------------------------------
// File: StagedRequest.hh
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

#ifndef QCLIENT_STAGED_REQUEST_HH
#define QCLIENT_STAGED_REQUEST_HH

#include "qclient/QCallback.hh"
#include "qclient/EncodedRequest.hh"

namespace qclient {

class StagedRequest {
public:
  StagedRequest(QCallback *cb, EncodedRequest &&request)
  : callback(cb), encodedRequest(std::move(request)) { }

  StagedRequest(const StagedRequest& other) = delete;
  StagedRequest(StagedRequest&& other) = delete;

  char* getBuffer() {
    return encodedRequest.getBuffer();
  }

  size_t getLen() const {
    return encodedRequest.getLen();
  }

  QCallback* getCallback() {
    return callback;
  }

  void set_value(redisReplyPtr &&reply) {
    if(callback) {
      callback->handleResponse(std::move(reply));
    }
  }

private:
  QCallback *callback = nullptr;
  EncodedRequest encodedRequest;
};

}

#endif
