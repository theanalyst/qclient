//------------------------------------------------------------------------------
// File: FutureHandler.cc
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

#include "qclient/FutureHandler.hh"

namespace qclient {

FutureHandler::FutureHandler() {}

FutureHandler::~FutureHandler() {}

std::future<redisReplyPtr> FutureHandler::stage() {
  std::unique_lock<std::mutex> lock(mtx);
  promises.emplace_back();
  lock.unlock();

  return promises.back().get_future();
}

void FutureHandler::handleResponse(redisReplyPtr &&reply) {
  promises.front().set_value(reply);
  std::lock_guard<std::mutex> lock(mtx);
  promises.pop_front();
}

}
