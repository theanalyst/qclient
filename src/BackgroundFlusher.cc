//------------------------------------------------------------------------------
// File: BackgroundFlusher.cc
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

#include "qclient/BackgroundFlusher.hh"
#include "qclient/Utils.hh"

using namespace qclient;

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
BackgroundFlusher::FlusherCallback::FlusherCallback(BackgroundFlusher *prnt) : parent(prnt) {}

void BackgroundFlusher::FlusherCallback::handleResponse(redisReplyPtr &&reply) {
  if(reply == nullptr) {

    //--------------------------------------------------------------------------
    // The only valid case where we might legitimately receive a nullptr is
    // during BackgroundFlusher shutdown.
    //--------------------------------------------------------------------------
    if(parent->inShutdown) {
      return;
    }

    //--------------------------------------------------------------------------
    // Nope, panic.
    //--------------------------------------------------------------------------
    parent->notifier.eventUnexpectedResponse("received nullptr in BackgroundFlusher::FlusherCallback::handleResponse, should never happen");
    std::terminate();
  }

  if(reply->type == REDIS_REPLY_ERROR) {
    std::string err(reply->str, reply->len);
    parent->notifier.eventUnexpectedResponse(SSTR("Unexpected backend response: " << err));
    std::terminate();
  }

  parent->itemWasAcknowledged();
}

BackgroundFlusher::~BackgroundFlusher() {
  inShutdown = true;
}

BackgroundFlusher::BackgroundFlusher(Members members, qclient::Options &&opts,
  Notifier &notif, BackgroundFlusherPersistency *pers)
: persistency(pers),
  callback(this),
  options(std::move(opts)),
  notifier(notif) {

  //----------------------------------------------------------------------------
  // Overwrite certain QClient options.
  //----------------------------------------------------------------------------
  options.transparentRedirects = true;
  options.retryStrategy = RetryStrategy::InfiniteRetries();

  //----------------------------------------------------------------------------
  // Initialize QClient object.
  //----------------------------------------------------------------------------
  qclient.reset(new QClient(members, std::move(options)));

  //----------------------------------------------------------------------------
  // Restore contents from persistency layer, if there are any.
  //----------------------------------------------------------------------------
  for(ItemIndex i = persistency->getStartingIndex(); i != persistency->getEndingIndex(); i++) {
    std::vector<std::string> contents;
    if(!persistency->retrieve(i, contents)) {
      std::cerr << "BackgroundFlusher corruption, could not retrieve entry with index " << i << std::endl;
      std::terminate();
    }

    qclient->execute(&callback, contents);
  }
}

size_t BackgroundFlusher::size() const {
  return persistency->getEndingIndex() - persistency->getStartingIndex();
}

// Return number of enqueued items since last time this function was called.
int64_t BackgroundFlusher::getEnqueuedAndClear() {
  int64_t retvalue = enqueued.exchange(0);
  return retvalue;
}

// Return number of acknowledged (dequeued) items since last time this function was called.
int64_t BackgroundFlusher::getAcknowledgedAndClear() {
  int64_t retvalue = acknowledged.exchange(0);
  return retvalue;
}

void BackgroundFlusher::pushRequest(const std::vector<std::string> &operation) {
  /*std::lock_guard<std::mutex> lock(newEntriesMtx);
  persistency->record(persistency->getEndingIndex(), operation);
  qclient->execute(&callback, operation);*/
  qhandler->pushRequest(operation);
  enqueued++;
}

void BackgroundFlusher::itemWasAcknowledged() {
  {
    std::lock_guard<std::mutex> lock(newEntriesMtx);
    persistency->pop();
  }
  acknowledged++;
  acknowledgementCV.notify_all();
}

void BackgroundFlusher::notifyWaiters()
{
  ++acknowledged;
  acknowledgementCV.notify_all();
}

BackgroundFlusher::SerialQueueHandler::SerialQueueHandler(BackgroundFlusher * parent): parent(parent), callback(&parent->callback) {}
void BackgroundFlusher::SerialQueueHandler::pushRequest(const std::vector<std::string>& operation)
{
  std::lock_guard lock(newEntriesMtx);
  parent->persistency->record(parent->persistency->getEndingIndex(), operation);
  parent->qclient->execute(callback, operation);
}