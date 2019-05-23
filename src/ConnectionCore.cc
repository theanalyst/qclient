//------------------------------------------------------------------------------
// File: ConnectionCore.cc
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

#include "ConnectionCore.hh"
#include "pubsub/MessageParser.hh"
#include "qclient/Handshake.hh"
#include "qclient/pubsub/MessageListener.hh"

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl;

namespace qclient {

ConnectionCore::ConnectionCore(Logger *log, Handshake *hs, BackpressureStrategy bp, RetryStrategy rs,
  MessageListener *ms)
: logger(log), handshake(hs), backpressure(bp), retryStrategy(rs), listener(ms) {
  reconnection();
}

ConnectionCore::~ConnectionCore() {

}

//------------------------------------------------------------------------------
// Check for "unavailable" response - specific to QDB
//------------------------------------------------------------------------------
static bool isUnavailable(redisReply* reply) {
  if(reply->type != REDIS_REPLY_ERROR) {
    return false;
  }

  static const std::string kFirstType("ERR unavailable");
  static const std::string kSecondType("UNAVAILABLE");

  if(strncmp(reply->str, kFirstType.c_str(), kFirstType.size()) == 0) {
    return true;
  }

  if(strncmp(reply->str, kSecondType.c_str(), kSecondType.size()) == 0) {
    return true;
  }

  return false;
}

void ConnectionCore::reconnection() {
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

void ConnectionCore::clearAllPending() {
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

void ConnectionCore::stage(QCallback *callback, EncodedRequest &&req, size_t multiSize) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);
  requestQueue.emplace_back(callback, std::move(req), multiSize);
}

std::future<redisReplyPtr> ConnectionCore::stage(EncodedRequest &&req, size_t multiSize) {
  std::lock_guard<std::mutex> lock(mtx);

  std::future<redisReplyPtr> retval = futureHandler.stage();
  requestQueue.emplace_back(&futureHandler, std::move(req), multiSize);
  return retval;
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> ConnectionCore::follyStage(EncodedRequest &&req, size_t multiSize) {
  backpressure.reserve();

  std::lock_guard<std::mutex> lock(mtx);

  folly::Future<redisReplyPtr> retval = follyFutureHandler.stage();
  requestQueue.emplace_back(&follyFutureHandler, std::move(req), multiSize);
  return retval;
}
#endif

void ConnectionCore::acknowledgePending(redisReplyPtr &&reply) {
  cbExecutor.stage(nextToAcknowledgeIterator.item().getCallback(), std::move(reply));
  discardPending();
}

void ConnectionCore::discardPending() {
  nextToAcknowledgeIterator.next();
  requestQueue.pop_front();
  backpressure.release();
}

bool ConnectionCore::consumeResponse(redisReplyPtr &&reply) {
  // Is this a transient "unavailable" error? Specific to QDB.
  if(retryStrategy.active() && isUnavailable(reply.get())) {
    // Break connection, try again.
    QCLIENT_LOG(logger, LogLevel::kWarn, "Cluster is temporarily unavailable: " << std::string(reply->str, reply->len));
    return false;
  }

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

  if(listener) {
    //--------------------------------------------------------------------------
    // We're in pub-sub mode, deliver replies to message listener.
    //--------------------------------------------------------------------------
    Message msg;
    if(!MessageParser::parse(std::move(reply), msg)) {
      //------------------------------------------------------------------------
      // Parse error, doesn't look like a valid pub/sub message
      //------------------------------------------------------------------------
      return false;
    }

    listener->handleIncomingMessage(std::move(msg));
    return true;
  }

  if(!nextToAcknowledgeIterator.itemHasArrived()) {
    //--------------------------------------------------------------------------
    // The server is sending more responses than we sent requests... wtf.
    // Break connection.
    //--------------------------------------------------------------------------
    QCLIENT_LOG(logger, LogLevel::kError, "Server is sending more responses than there were requests ?!?");
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

void ConnectionCore::setBlockingMode(bool value) {
  handshakeRequests.setBlockingMode(value);
  requestQueue.setBlockingMode(value);
}

StagedRequest* ConnectionCore::getNextToWrite() {
  if(inHandshake) {
    StagedRequest *item = handshakeIterator.getItemBlockOrNull();
    if(!item) return nullptr;

    handshakeIterator.next();
    return item;
  }

  StagedRequest *item = nextToWriteIterator.getItemBlockOrNull();

  if (listener) {
    //--------------------------------------------------------------------------
    // The connection is in pub-sub mode, which means normal requests are no
    // longer being acknowledged. The request queue can potentially grow to
    // infinity - let's trim no-longer-needed items.
    //--------------------------------------------------------------------------
    while(nextToWriteIterator.seq() > nextToAcknowledgeIterator.seq()) {
      discardPending();
    }
  }

  if(!item) return nullptr;
  nextToWriteIterator.next();
  return item;
}

}
