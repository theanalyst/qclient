//------------------------------------------------------------------------------
// File: MultiBuilder.hh
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

#ifndef QCLIENT_MULTI_BUILDER_HH
#define QCLIENT_MULTI_BUILDER_HH

#include "EncodedRequest.hh"

namespace qclient {

class MultiBuilder {
public:

  template<typename... Args>
  void emplace_back(const Args&... args) {
    contents.emplace_back(EncodedRequest::make(args...));
  }

  size_t size() const {
    return contents.size();
  }

  std::deque<EncodedRequest>&& getDeque() {
    return std::move(contents);
  }

private:
  std::deque<EncodedRequest> contents;
};

}

#endif