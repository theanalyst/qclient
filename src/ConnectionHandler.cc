//------------------------------------------------------------------------------
// File: ConnectionHandler.cc
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

#include "ConnectionHandler.hh"

namespace qclient {

ConnectionHandler::ConnectionHandler(Handshake *hs, BackpressureStrategy bp)
: backpressure(bp), handshake(hs) {
  reconnection();
}

ConnectionHandler::~ConnectionHandler() {

}

void ConnectionHandler::reconnection() {
  //----------------------------------------------------------------------------
  // The connection has dropped. This means:
  // - We're in handshake mode once again - forbidden to process user requests
  //   until handshake has completed.
  // - Any requests written onto the socket which have not been acknowledged
  //   yet will have to be processed anew.
  // - Any un-acknowledged transactions will have to start from scratch.
  //
  // We may or may not purge un-acknowledged requests without trying them
  // again, but that's for clearAllPending() to decide, not us.
  //----------------------------------------------------------------------------

  if(handshake) {
    //--------------------------------------------------------------------------
    // Re-initialize handshake.
    //--------------------------------------------------------------------------
    inHandshake = true;

    handshake->restart();
    handshakeRequests.reset();
    handshakeRequests.emplace_back(nullptr, handshake->provideHandshake());
    handshakeIterator = handshakeRequests.begin();
  }
  else {
    inHandshake = false;
  }

  //----------------------------------------------------------------------------
  // Re-initialize ignored responses for transactions, reset iterators.
  //----------------------------------------------------------------------------
  ignoredResponses = 0u;
  nextToWriteIterator = requestQueue.begin();
  nextToAcknowledgeIterator = requestQueue.begin();
}

void ConnectionHandler::clearAllPending() {
  std::lock_guard<std::mutex> lock(mtx);

  //----------------------------------------------------------------------------
  // The party's over, any requests that still remain un-acknowledged
  // will get a null response.
  //----------------------------------------------------------------------------
  inHandshake = false;

  redisReplyPtr nullReply;
  while(nextToAcknowledgeIterator.itemHasArrived()) {
    acknowledgePending(std::move(nullReply));
  }

  requestQueue.reset();
  reconnection();
}

void ConnectionHandler::stage(QCallback *callback, EncodedRequest &&req, size_t multiSize) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);
  requestQueue.emplace_back(callback, std::move(req), multiSize);
}

std::future<redisReplyPtr> ConnectionHandler::stage(EncodedRequest &&req, bool bypassBackpressure, size_t multiSize) {
  if(!bypassBackpressure) {
    backpressure.reserve();
  }

  std::lock_guard<std::mutex> lock(mtx);

  std::future<redisReplyPtr> retval = futureHandler.stage();
  requestQueue.emplace_back(&futureHandler, std::move(req), multiSize);
  return retval;
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> ConnectionHandler::follyStage(EncodedRequest &&req, size_t multiSize) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);

  folly::Future<redisReplyPtr> retval = follyFutureHandler.stage();
  requestQueue.emplace_back(&follyFutureHandler, std::move(req), multiSize);
  return retval;
}
#endif

void ConnectionHandler::acknowledgePending(redisReplyPtr &&reply) {
  cbExecutor.stage(nextToAcknowledgeIterator.item().getCallback(), std::move(reply));
  nextToAcknowledgeIterator.next();
  requestQueue.pop_front();
  backpressure.release();
}

bool ConnectionHandler::consumeResponse(redisReplyPtr &&reply) {
  // Is this a response to the handshake?
  if(inHandshake) {
    // Forward reply to handshake object, and check the response.
    Handshake::Status status = handshake->validateResponse(reply);

    if(status == Handshake::Status::INVALID) {
      // Error during handshaking, drop connection
      return false;
    }

    if(status == Handshake::Status::VALID_COMPLETE) {
      // We're done handshaking
      inHandshake = false;
      handshakeRequests.setBlockingMode(false);
      return true;
    }

    if(status == Handshake::Status::VALID_INCOMPLETE) {
      // Still more requests to go
      handshakeRequests.emplace_back(nullptr, handshake->provideHandshake());
      return true;
    }

    qclient_assert("should never happen");
  }

  if(!nextToAcknowledgeIterator.itemHasArrived()) {
    //--------------------------------------------------------------------------
    // The server is sending more responses than we sent requests.. wtf. Break
    // connection.
    // TODO: Log warning
    //--------------------------------------------------------------------------
    return false;
  }

  if(nextToAcknowledgeIterator.item().getMultiSize() != 0u) {
    ignoredResponses++;

    if(ignoredResponses <= nextToAcknowledgeIterator.item().getMultiSize()) {
      //------------------------------------------------------------------------
      // This is a QUEUED response, send it into a black hole
      // TODO: verify this is indeed QUEUED, lol
      //------------------------------------------------------------------------
      return true;
    }

    // This is the real response.
    ignoredResponses = 0u;
  }

  acknowledgePending(std::move(reply));
  return true;
}

void ConnectionHandler::setBlockingMode(bool value) {
  handshakeRequests.setBlockingMode(value);
  requestQueue.setBlockingMode(value);
}

StagedRequest* ConnectionHandler::getNextToWrite() {
  if(inHandshake) {
    StagedRequest *item = handshakeIterator.getItemBlockOrNull();
    if(!item) return nullptr;

    handshakeIterator.next();
    return item;
  }

  StagedRequest *item = nextToWriteIterator.getItemBlockOrNull();
  if(!item) return nullptr;
  nextToWriteIterator.next();
  return item;
}

}
