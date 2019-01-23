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

#include "FutureHandler.hh"


namespace qclient {

#if HAVE_FOLLY == 1
FollyFutureHandler::FollyFutureHandler() {}

FollyFutureHandler::~FollyFutureHandler() {}

folly::Future<redisReplyPtr> FollyFutureHandler::stage() {
  folly::Promise<redisReplyPtr> prom;
  folly::Future<redisReplyPtr> retval = prom.getFuture();

  promises.emplace_back(std::move(prom));
  return retval;
}

void FollyFutureHandler::handleResponse(redisReplyPtr &&reply) {
  promises.front().setValue(std::move(reply));
  promises.pop_front();
}

#endif

FutureHandler::FutureHandler() {}

FutureHandler::~FutureHandler() {}

std::future<redisReplyPtr> FutureHandler::stage() {
  std::promise<redisReplyPtr> prom;
  std::future<redisReplyPtr> retval = prom.get_future();

  promises.emplace_back(std::move(prom));
  return retval;
}

void FutureHandler::handleResponse(redisReplyPtr &&reply) {
  promises.front().set_value(reply);
  promises.pop_front();
}

}
