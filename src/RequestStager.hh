//------------------------------------------------------------------------------
// File: RequestStager.hh
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

#ifndef QCLIENT_REQUEST_STAGER_HH
#define QCLIENT_REQUEST_STAGER_HH

#include "qclient/ThreadSafeQueue.hh"
#include "RequestQueue.hh"
#include "StagedRequest.hh"
#include "FutureHandler.hh"
#include "BackpressureApplier.hh"
#include "qclient/Handshake.hh"
#include "RequestQueue.hh"
#include "CallbackExecutorThread.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Handles all pending, still un-acknowledged requests. We need this to support
// retries, if the connection shuts down.
//------------------------------------------------------------------------------
class RequestStager {
public:
  RequestStager(BackpressureStrategy backpressure);
  ~RequestStager();

  void stage(QCallback *callback, EncodedRequest &&req, size_t multiSize = 0u);
  std::future<redisReplyPtr> stage(EncodedRequest &&req, bool bypassBackpressure = false, size_t multiSize = 0u);
#if HAVE_FOLLY == 1
  folly::Future<redisReplyPtr> follyStage(EncodedRequest &&req, size_t multiSize = 0u);
#endif

  // Returns false if there's no staged request to satisfy, true otherwise.
  bool consumeResponse(redisReplyPtr &&reply);
  void clearAllPending();
  void reconnection();

  //----------------------------------------------------------------------------
  // Forward to the underlying queue.
  //----------------------------------------------------------------------------
  void setBlockingMode(bool value) {
    stagedRequests.setBlockingMode(value);
  }

  //----------------------------------------------------------------------------
  // Get iterator at the front of the queue, bypassing the first hidden
  // element.
  //----------------------------------------------------------------------------
  RequestQueue::Iterator getIterator();

private:
  size_t ignoredResponses = 0u;
  void restoreInvariant();
  RequestQueue::Iterator nextToAcknowledgeIterator;
  RequestQueue stagedRequests;
  BackpressureApplier backpressure;

  FutureHandler futureHandler;

#if HAVE_FOLLY == 1
  FollyFutureHandler follyFutureHandler;
#endif

  // NOTE: cbExecutor must be destroyed _after_ FutureHandler, so it has to be
  // below it in the member variables definition.
  CallbackExecutorThread cbExecutor;

  std::mutex mtx;
};

}


#endif
