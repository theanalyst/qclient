//------------------------------------------------------------------------------
// File: ConnectionCore.hh
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

#ifndef QCLIENT_CONNECTION_HANDLER_HH
#define QCLIENT_CONNECTION_HANDLER_HH

#include "qclient/WaitableQueue.hh"
#include "BackpressureApplier.hh"
#include "RequestQueue.hh"
#include "FutureHandler.hh"
#include "CallbackExecutorThread.hh"
#include "qclient/Logger.hh"
#include "qclient/MessageListener.hh"

namespace qclient {

class Handshake;

//------------------------------------------------------------------------------
// Handles a particular connection, deciding what should be written into the
// socket, and consumes bytes out of it. However, this class is decoupled from
// the actual networking code.
//------------------------------------------------------------------------------
class ConnectionCore {
public:
  ConnectionCore(Logger *log, Handshake *hs, BackpressureStrategy backpressure, RetryStrategy rs);
  ~ConnectionCore();
  void reconnection();

  // Returns whether connection is still alive after consuming this response.
  // False can happen durnig a failed handshake, for example.
  bool consumeResponse(redisReplyPtr &&reply);

  void stage(QCallback *callback, EncodedRequest &&req, size_t multiSize = 0u);
  std::future<redisReplyPtr> stage(EncodedRequest &&req, size_t multiSize = 0u);

#if HAVE_FOLLY == 1
  folly::Future<redisReplyPtr> follyStage(EncodedRequest &&req, size_t multiSize = 0u);
#endif

  void setBlockingMode(bool value);
  StagedRequest* getNextToWrite();
  void clearAllPending();
  void enterSubscriptionMode(MessageListener *listener);
  void trimQueueDuringPubsub();

private:
  Logger *logger;
  Handshake *handshake;
  BackpressureApplier backpressure;
  RetryStrategy retryStrategy;
  MessageListener *listener = nullptr;

  bool enteredPubsub = false;
  std::atomic<int64_t> pubsubThreshold {-1};

  void acknowledgePending(redisReplyPtr &&reply);
  size_t ignoredResponses = 0u;

  WaitableQueue<StagedRequest, 15> handshakeRequests;
  decltype(handshakeRequests)::Iterator handshakeIterator;

  std::atomic<bool> inHandshake {true};
  RequestQueue::Iterator nextToWriteIterator;
  RequestQueue::Iterator nextToAcknowledgeIterator;
  RequestQueue requestQueue;

  FutureHandler futureHandler;

#if HAVE_FOLLY == 1
  FollyFutureHandler follyFutureHandler;
#endif

  // NOTE: cbExecutor must be destroyed before FutureHandler, so it has to be
  // below it in the member variables definition.
  CallbackExecutorThread cbExecutor;

  std::mutex mtx;
};

}

#endif
