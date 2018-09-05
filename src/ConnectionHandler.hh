//------------------------------------------------------------------------------
// File: ConnectionHandler.hh
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

#include "RequestStager.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Handles a particular connection, deciding what should be written into the
// socket, and consumes bytes out of it. However, this class is decoupled from
// the actual networking code.
//------------------------------------------------------------------------------
class ConnectionHandler {
public:
  ConnectionHandler(Handshake *hs, BackpressureStrategy backpressure);
  ~ConnectionHandler();
  void reconnection();

  // Returns whether connection is still alive after consuming this response.
  // False can happen durnig a failed handshake, for example.
  bool consumeResponse(redisReplyPtr &&reply);

  void stage(QCallback *callback, EncodedRequest &&req, size_t multiSize = 0u);
  std::future<redisReplyPtr> stage(EncodedRequest &&req, bool bypassBackpressure = false, size_t multiSize = 0u);

#if HAVE_FOLLY == 1
  folly::Future<redisReplyPtr> follyStage(EncodedRequest &&req, size_t multiSize = 0u);
#endif

  void setBlockingMode(bool value);
  StagedRequest* getNextToWrite();
  void clearAllPending();

private:
  void acknowledgePending(redisReplyPtr &&reply);

  size_t ignoredResponses = 0u;
  BackpressureApplier backpressure;

  WaitableQueue<StagedRequest, 15> handshakeRequests;
  decltype(handshakeRequests)::Iterator handshakeIterator;
  Handshake *handshake;

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
