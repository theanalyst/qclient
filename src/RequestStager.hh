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
#include "StagedRequest.hh"
#include "FutureHandler.hh"
#include "BackpressureApplier.hh"
#include "qclient/Handshake.hh"
#include "CallbackExecutorThread.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Handles all pending, still un-acknowledged requests. We need this to support
// retries, if the connection shuts down.
//
// INVARIANT: At all times, this class holds one extra, hidden request at
// the front of the queue.
//
// The writer event loop may still be accessing the front element, while the
// reader loop has already received a response. We give a leeway of a single
// extra request before deallocating stuff, even after it has been satisfied to
// allow the writer loop to progress safely.
//
// EXAMPLE:
// - Writer loop does a ::send with the top request.
// - Response comes very quickly, the reader loop calls satisfy().
// - If we were to free that item now, the writer loop might segfault as it
//   has to access that item again after ::send.
//
// Could there be more responses coming? Well, no, as the writer thread hasn't
// sent those yet.
//
// To prevent the above, we always keep around one extra item at the front of
// the queue, but the interface does not reflect this: Only this class knows
// that this is happening.
//
// getIterator will thus return the second item, instead of the first.
//
// To be able to hold this invariant even at startup and keep the code simple,
// we insert a dummy request during construction.
//------------------------------------------------------------------------------
class RequestStager {
public:
  using QueueType = ThreadSafeQueue<StagedRequest, 5000>;

  RequestStager(BackpressureStrategy backpressure);
  ~RequestStager();

  void stage(QCallback *callback, EncodedRequest &&req);
  std::future<redisReplyPtr> stage(EncodedRequest &&req, bool bypassBackpressure = false);
#if HAVE_FOLLY == 1
  folly::Future<redisReplyPtr> follyStage(EncodedRequest &&req);
#endif

  void satisfy(redisReplyPtr &&reply);
  void clearAllPending();

  //----------------------------------------------------------------------------
  // If blocking mode is set to false, "blockUntilStaged" does not actually
  // block.
  //
  // Set it to false whenever you want to unblock threads waiting inside
  // that function, such as during shutdown.
  //
  // Set it to true before activating some event loop that has to wait on
  // incoming requests.
  //----------------------------------------------------------------------------
  void setBlockingMode(bool value);

  //----------------------------------------------------------------------------
  // Block until a request newer than requestID has been staged.
  //----------------------------------------------------------------------------
  void blockUntilStaged(int64_t requestID);

  int64_t getHighestRequestId() {
    return highestRequestID;
  }

  //----------------------------------------------------------------------------
  // Get iterator at the front of the queue, bypassing the first hidden
  // element.
  //----------------------------------------------------------------------------
  QueueType::Iterator getIterator();

private:
  void restoreInvariant();
  QueueType::Iterator nextToAcknowledgeIterator;

  std::atomic<bool> blockingMode {true};

  BackpressureApplier backpressure;
  QueueType stagedRequests;

  FutureHandler futureHandler;

#if HAVE_FOLLY == 1
  FollyFutureHandler follyFutureHandler;
#endif

  // NOTE: cbExecutor must be destroyed _after_ FutureHandler, so it has to be
  // below it in the member variables definition.
  CallbackExecutorThread cbExecutor;

  std::mutex mtx;
  std::condition_variable cv;
  std::atomic<int64_t> highestRequestID { -1 };
};

}


#endif
