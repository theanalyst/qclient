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

ConnectionHandler::ConnectionHandler(Handshake *hs, BackpressureStrategy backpressure)
: handshake(hs), requestStager(backpressure) {
  reconnection();
}

ConnectionHandler::~ConnectionHandler() {

}

void ConnectionHandler::reconnection() {
  if(handshake) {
    inHandshake = true;

    handshake->restart();
    handshakeRequests.reset();
    handshakeRequests.emplace_back(nullptr, handshake->provideHandshake());
    handshakeIterator = handshakeRequests.begin();
  }
  else {
    inHandshake = false;
  }

  stagerIterator = requestStager.getIterator();
}

void ConnectionHandler::clearAllPending() {
  requestStager.clearAllPending();
  stagerIterator = requestStager.getIterator();
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

  requestStager.consumeResponse(std::move(reply));
  return true;
}

void ConnectionHandler::setBlockingMode(bool value) {
  handshakeRequests.setBlockingMode(value);
  requestStager.setBlockingMode(value);
}

StagedRequest* ConnectionHandler::getNextToWrite() {
  if(inHandshake) {
    if(!handshakeIterator.itemHasArrived()) {
      handshakeIterator.blockUntilItemHasArrived();
    }

    if(!handshakeIterator.itemHasArrived()) {
      return nullptr;
    }

    StagedRequest *item = &handshakeIterator.item();
    handshakeIterator.next();
    return item;
  }

  if(!stagerIterator.itemHasArrived()) {
    stagerIterator.blockUntilItemHasArrived();
  }

  if(!stagerIterator.itemHasArrived()) {
    return nullptr;
  }

  StagedRequest *item = &stagerIterator.item();
  stagerIterator.next();
  return item;
}

}
