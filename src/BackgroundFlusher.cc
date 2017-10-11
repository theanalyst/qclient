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
  size_t szLimit, size_t pipeline, BackgroundFlusherPersistency *persistency)
: queue(persistency, szLimit), qclient(qcl), notifier(notif), pipelineLength(pipeline),
  thread(&BackgroundFlusher::main, this) { }

size_t BackgroundFlusher::size() const {
  return queue.size();
}

// Return
int64_t BackgroundFlusher::getEnqueuedAndClear() {
  int64_t retvalue = enqueued.exchange(0);
  return retvalue;
}

void BackgroundFlusher::pushRequest(const std::vector<std::string> &operation) {
  PushStatus status = queue.push(operation);
  if(!status.ok) {
    std::cerr << "could not append item to queue. Wait for: " << status.blockedFor.count() << std::endl;
    std::terminate();
  }
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

bool BackgroundFlusher::checkPendingQueue(std::list<std::future<redisReplyPtr>> &inflight) {
  while(true) {
    if(inflight.size() == 0) return true;
    if(!is_ready(inflight.front())) return true;

    redisReplyPtr reply = inflight.front().get();
    inflight.pop_front();

    if(!verifyReply(reply)) {
      return false;
    }
    queue.pop();
  }

  return true;
}

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

void BackgroundFlusher::processPipeline(ThreadAssistant &assistant) {
  // When in this function, we know the connection is stable, so we can push
  // out many updates at a time.

  std::list<std::future<redisReplyPtr>> inflight;
  auto pipelineFrontier = queue.begin();

  while(!assistant.terminationRequested()) {
    // Have any of the responses arrived?
    if(!checkPendingQueue(inflight)) {
      // Error, return to parent. All inFlight requests are discarded,
      // we'll send them again in the next round.
      return;
    }

    // Can I push out one more item?
    if(inflight.size() < pipelineLength && inflight.size() < queue.size()) {
      // Adjust pipelineFrontier
      if(inflight.size() == 0) {
        pipelineFrontier = queue.begin();
      }
      else {
        pipelineFrontier++;
      }

      inflight.push_back(qclient.execute(*pipelineFrontier));
    }
    else {
      // No - why not?
      // 1. I've reached the pipelineLength limit. In such case, block on receiving
      //    a response.
      if(pipelineLength <= inflight.size()) {
        inflight.front().wait_for(std::chrono::milliseconds(500));
      }
      // 2. No more entries to push. Wait until more are received.
      else if(inflight.size() == queue.size()){
        queue.wait_for(inflight.size(), std::chrono::milliseconds(500));
      }
    }
  }
}

void BackgroundFlusher::main(ThreadAssistant &assistant) {
  // When inside this loop, we aren't exactly sure if the connection is stable.
  // First send a single request and wait for a response, before launching
  // a pipeline.

  while(!assistant.terminationRequested()) {
    // Are there entries to push? If not, wait until more are received.
    if(queue.size() == 0) {
      queue.wait_for(0, std::chrono::milliseconds(500));
      continue;
    }

    // There are! Send the top one, wait maximum 2 sec for reply..
    std::future<redisReplyPtr> future = qclient.execute(queue.top());
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
    queue.pop();
    processPipeline(assistant);
  }
}
