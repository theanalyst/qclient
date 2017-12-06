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

BackgroundFlusher::BackgroundFlusher(QClient &qcl, Notifier &notif,
  size_t szLimit, size_t pipeline, BackgroundFlusherPersistency *pers)
: persistency(pers), qclient(qcl), notifier(notif), pipelineLength(pipeline),
  thread(&BackgroundFlusher::main, this) { }

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
  std::lock_guard<std::mutex> lock(newEntriesMtx);
  persistency->record(persistency->getEndingIndex(), operation);
  newEntriesCV.notify_all();
  enqueued++;
}

static bool is_ready(std::future<redisReplyPtr> &fut) {
  return fut.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
}

static bool startswith(const std::string &str, const std::string &prefix) {
  if(prefix.size() > str.size()) return false;

  for(size_t i = 0; i < prefix.size(); i++) {
    if(str[i] != prefix[i]) return false;
  }
  return true;
}

bool BackgroundFlusher::verifyReply(redisReplyPtr &reply) {
  if(reply == nullptr) {
    notifier.eventNetworkIssue("connection error");
    return false;
  }

  if(reply->type == REDIS_REPLY_ERROR) {
    std::string err(reply->str, reply->len);

    if(startswith(err, "unavailable")) {
      notifier.eventNetworkIssue(err);
    }
    else {
      notifier.eventUnexpectedResponse(err);
    }

    return false;
  }

  return true;
}

void BackgroundFlusher::itemWasAcknowledged() {
  std::lock_guard<std::mutex> lock(acknowledgementMtx);
  persistency->pop();
  acknowledged++;
  acknowledgementCV.notify_all();
}

bool BackgroundFlusher::checkPendingQueue(std::list<std::future<redisReplyPtr>> &inflight) {
  while(true) {
    if(inflight.size() == 0) return true;
    if(!is_ready(inflight.front())) return true;

    redisReplyPtr reply = inflight.front().get();
    inflight.pop_front();

    if(!verifyReply(reply)) {
      return false;
    }

    itemWasAcknowledged();
  }

  return true;
}

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

void BackgroundFlusher::monitorAckReception(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    std::unique_lock<std::mutex> lock(inFlightMtx);

    if(inFlight.size() == 0) {
      // Empty queue, sleep
      inFlightCV.wait_for(lock, std::chrono::milliseconds(500));
      continue;
    }

    // Fetch reference to top item
    std::future<redisReplyPtr> &item = inFlight.front();
    lock.unlock();

    if(item.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready) {
      continue;
    }

    redisReplyPtr response = item.get();
    if(!verifyReply(response)) {
      // Stop the pipeline, we have an error
      break;
    }

    // All clear, acknowledgement was OK.
    lock.lock();

    inFlight.pop_front();
    itemWasAcknowledged();
  }

  haltPipeline = true;
}

void BackgroundFlusher::processPipeline(ThreadAssistant &assistant) {
  inFlight.clear();
  haltPipeline = false;
  AssistedThread ackmonitor(&BackgroundFlusher::monitorAckReception, this);

  // When in this function, we know the connection is stable, so we can push
  // out many updates at a time.

  ItemIndex currentIndex = persistency->getStartingIndex();

  while(!assistant.terminationRequested() && !haltPipeline) {
    std::unique_lock<std::mutex> lock(inFlightMtx);

    // Can I push out one more item?
    if(inFlight.size() < pipelineLength && currentIndex < persistency->getEndingIndex())  {
      lock.unlock();

      std::vector<std::string> contents;
      if(!persistency->retrieve(currentIndex, contents)) {
        std::cerr << "BackgroundFlusher corruption, could not retrieve entry with index " << currentIndex << std::endl;
        std::terminate();
      }

      currentIndex++;
      std::future<redisReplyPtr> fut = qclient.execute(contents);

      lock.lock();
      inFlight.push_back(std::move(fut));
      if(inFlight.size() == 0) {
        inFlightCV.notify_one();
      }
    }
    else {
      // No - why not?
      // 1. I've reached the pipelineLength limit. In such case, block on receiving
      //    a response.
      if(pipelineLength <= inFlight.size()) {
        acknowledgementCV.wait_for(lock, std::chrono::milliseconds(500));
        continue;
      }

      // 2. No more entries to push. Wait until more are received.
      lock.unlock();
      std::unique_lock<std::mutex> lock2(newEntriesMtx);
      if(currentIndex < persistency->getEndingIndex()) {
        continue;
      }

      newEntriesCV.wait_for(lock2, std::chrono::milliseconds(500));
    }
  }
}

void BackgroundFlusher::main(ThreadAssistant &assistant) {
  // When inside this loop, we aren't exactly sure if the connection is stable.
  // First send a single request and wait for a response, before launching
  // a pipeline.

  while(!assistant.terminationRequested()) {
    // Are there entries to push? If not, wait until more are received.
    if(persistency->getStartingIndex() == persistency->getEndingIndex()) {
      this->waitForIndex(persistency->getEndingIndex(), std::chrono::milliseconds(500));
      continue;
    }

    std::vector<std::string> contents;
    if(!persistency->retrieve(persistency->getStartingIndex(), contents)) {
      std::cerr << "BackgroundFlusher corruption, could not retrieve entry with index " << persistency->getStartingIndex() << std::endl;
      std::terminate();
    }

    // There are! Send the top one, wait maximum 2 sec for reply..
    std::future<redisReplyPtr> future = qclient.execute(contents);
    if(future.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
      continue;
    }

    // We have a reply, verify it
    redisReplyPtr reply = future.get();
    if(!verifyReply(reply)) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }

    // All is well, launch the pipeline
    itemWasAcknowledged();
    processPipeline(assistant);
  }
}
