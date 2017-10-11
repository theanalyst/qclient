//------------------------------------------------------------------------------
// File: BackgroundFlusher.hh
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

#ifndef __QCLIENT_BACKGROUND_FLUSHER_H__
#define __QCLIENT_BACKGROUND_FLUSHER_H__

#include "qclient/QClient.hh"
#include "qclient/BackpressuredQueue.hh"
#include "qclient/AssistedThread.hh"

namespace qclient {

// Interface to notify whenever the background flusher encounters some error.
// If you inherit from this object, make sure your implementation doesn't block
// the calling thread for too long..
class Notifier {
public:
  virtual void eventNetworkIssue(const std::string &err) {}
  virtual void eventUnexpectedResponse(const std::string &err) {}
  virtual void eventShutdown() {}
};

class ResponseVerifier {
  virtual void callback(const std::vector<std::string> &request, const redisReplyPtr &response);
};

using BackgroundFlusherPersistency = PersistencyLayer<std::vector<std::string>>;

class BackgroundFlusher {
public:
  BackgroundFlusher(QClient &client, Notifier &notifier, size_t sizeLimit, size_t pipelineLength,
    BackgroundFlusherPersistency *persistency = nullptr);

  int64_t getEnqueuedAndClear();
  void pushRequest(const std::vector<std::string> &operation);
  size_t size() const;
private:
  BackpressuredQueue<std::vector<std::string>, BackpressureStrategyLimitSize> queue;

  QClient &qclient;
  Notifier &notifier;

  size_t pipelineLength;
  AssistedThread thread;
  std::atomic<int64_t> enqueued {0};

  void main(ThreadAssistant &assistant);
  void processPipeline(ThreadAssistant &assistant);
  bool checkPendingQueue(std::list<std::future<redisReplyPtr>> &inflight);
  bool verifyReply(redisReplyPtr &reply);
};

}

#endif
