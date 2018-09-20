//------------------------------------------------------------------------------
// File: FutureHandler.hh
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

#ifndef QCLIENT_FUTURE_HANDLER_H
#define QCLIENT_FUTURE_HANDLER_H

#include "qclient/QCallback.hh"
#include "qclient/ThreadSafeQueue.hh"
#include <future>

#if HAVE_FOLLY == 1
#include <folly/futures/Future.h>
#endif

namespace qclient {

#if HAVE_FOLLY == 1
class FollyFutureHandler : public QCallback {
public:
  FollyFutureHandler();
  virtual ~FollyFutureHandler();

  folly::Future<redisReplyPtr> stage();
  virtual void handleResponse(redisReplyPtr &&reply) override;
private:
  ThreadSafeQueue<folly::Promise<redisReplyPtr>, 5000> promises;
};
#endif

class FutureHandler : public QCallback {
public:
  FutureHandler();
  virtual ~FutureHandler();

  std::future<redisReplyPtr> stage();
  virtual void handleResponse(redisReplyPtr &&reply) override;

private:
  ThreadSafeQueue<std::promise<redisReplyPtr>, 5000> promises;
};

}

#endif
