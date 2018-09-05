//------------------------------------------------------------------------------
// File: RequestStager.cc
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

#include "RequestStager.hh"

namespace qclient {

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

RequestStager::RequestStager(BackpressureStrategy backpr) : backpressure(backpr) {
  restoreInvariant();
}

RequestStager::~RequestStager() {
  clearAllPending();
}

std::future<redisReplyPtr> RequestStager::stage(EncodedRequest &&req, bool bypassBackpressure, size_t multiSize) {
  if(!bypassBackpressure) {
    backpressure.reserve();
  }

  std::lock_guard<std::mutex> lock(mtx);

  std::future<redisReplyPtr> retval = futureHandler.stage();
  stagedRequests.emplace_back(&futureHandler, std::move(req), multiSize);
  return retval;
}

void RequestStager::stage(QCallback *callback, EncodedRequest &&req, size_t multiSize) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);
  stagedRequests.emplace_back(callback, std::move(req), multiSize);
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> RequestStager::follyStage(EncodedRequest &&req, size_t multiSize) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);

  folly::Future<redisReplyPtr> retval = follyFutureHandler.stage();
  stagedRequests.emplace_back(&follyFutureHandler, std::move(req), multiSize);
  return retval;
}
#endif

void RequestStager::clearAllPending() {
  std::lock_guard<std::mutex> lock(mtx);

  // The party's over, any requests that still remain un-acknowledged
  // will get a null response.

  while(nextToAcknowledgeIterator.itemHasArrived()) {
    consumeResponse(redisReplyPtr());
  }

  restoreInvariant();
}

void RequestStager::reconnection() {
  ignoredResponses = 0u;
}

void RequestStager::restoreInvariant() {
  // Restore class invariant: Insert dummy element to always have that
  // nextToAcknowledgeIterator == stagedRequests.begin() + 1

  ignoredResponses = 0u;
  stagedRequests.reset();
  nextToAcknowledgeIterator = stagedRequests.begin();
}

bool RequestStager::consumeResponse(redisReplyPtr &&reply) {
  if(!nextToAcknowledgeIterator.itemHasArrived()) {
    return false;
  }

  if(nextToAcknowledgeIterator.item().getMultiSize() != 0u) {
    ignoredResponses++;

    if(ignoredResponses <= nextToAcknowledgeIterator.item().getMultiSize()) {
      // This is a QUEUED response, send it into a black hole
      return true;
    }

    // This is the real response.
    ignoredResponses = 0u;
  }

  cbExecutor.stage(nextToAcknowledgeIterator.item().getCallback(), std::move(reply));
  nextToAcknowledgeIterator.next();
  stagedRequests.pop_front();
  backpressure.release();
  return true;
}

RequestQueue::Iterator RequestStager::getIterator() {
  return stagedRequests.begin();
}

}
