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

std::future<redisReplyPtr> RequestStager::stage(EncodedRequest &&req, bool bypassBackpressure) {
  if(!bypassBackpressure) {
    backpressure.reserve();
  }

  std::lock_guard<std::mutex> lock(mtx);

  std::future<redisReplyPtr> retval = futureHandler.stage();
  stagedRequests.emplace_back(&futureHandler, std::move(req));
  return retval;
}

void RequestStager::stage(QCallback *callback, EncodedRequest &&req) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);
  stagedRequests.emplace_back(callback, std::move(req));
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> RequestStager::follyStage(EncodedRequest &&req) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);

  folly::Future<redisReplyPtr> retval = follyFutureHandler.stage();
  stagedRequests.emplace_back(&follyFutureHandler, std::move(req));
  return retval;
}
#endif

void RequestStager::clearAllPending() {
  std::lock_guard<std::mutex> lock(mtx);

  // The party's over, any requests that still remain un-acknowledged
  // will get a null response.

  while(nextToAcknowledgeIterator.itemHasArrived()) {
    satisfy(redisReplyPtr());
  }

  restoreInvariant();
}

void RequestStager::restoreInvariant() {
  // Restore class invariant: Insert dummy element to always have that
  // nextToAcknowledgeIterator == stagedRequests.begin() + 1

  stagedRequests.reset();

  stagedRequests.emplace_back(nullptr, EncodedRequest(std::vector<std::string>{"dummy"}));
  nextToAcknowledgeIterator = stagedRequests.begin();
  nextToAcknowledgeIterator.next();
}

void RequestStager::satisfy(redisReplyPtr &&reply) {
  cbExecutor.stage(nextToAcknowledgeIterator.item().getCallback(), std::move(reply));
  nextToAcknowledgeIterator.next();
  stagedRequests.pop_front();
  backpressure.release();
}

RequestStager::QueueType::Iterator RequestStager::getIterator() {
  auto iter = stagedRequests.begin();
  iter.next();
  return iter;
}

}
