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
#include "qclient/AssistedThread.hh"

namespace qclient {

//------------------------------------------------------------------------------
// PersistencyLayer interface - inherit from here to implement extra
// functionality. Default implementation does nothing at all.
//------------------------------------------------------------------------------
using ItemIndex = int64_t;

template<typename QueueItem>
class PersistencyLayer {
public:
  PersistencyLayer() {}
  virtual ~PersistencyLayer() {} // very important to be virtual!

  virtual void record(ItemIndex index, const QueueItem &item) {}
  virtual void pop() {}

  // The following three functions are only used during reconstruction.
  virtual ItemIndex getStartingIndex() {
    return 0;
  }

  virtual ItemIndex getEndingIndex() {
    return 0;
  }

  virtual bool retrieve(ItemIndex index, QueueItem &ret) {
    return false;
  }
};

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
  BackgroundFlusher(Members members, qclient::Options &&options, Notifier &notifier,
    BackgroundFlusherPersistency *persistency = nullptr);

  ~BackgroundFlusher();

  int64_t getEnqueuedAndClear();
  int64_t getAcknowledgedAndClear();

  void pushRequest(const std::vector<std::string> &operation);
  size_t size() const;

  bool hasItemBeenAcked(ItemIndex index) {
    return (index < persistency->getStartingIndex());
  }

  template<typename Duration>
  bool waitForIndex(ItemIndex index, Duration duration) {
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    auto deadline = start + duration;

    std::unique_lock<std::mutex> lock(acknowledgementMtx);
    while(std::chrono::steady_clock::now() < deadline) {
      if(hasItemBeenAcked(index)) return true;
      acknowledgementCV.wait_for(lock, duration);
    }

    return hasItemBeenAcked(index);
  }

  ItemIndex getEndingIndex() {
    return persistency->getEndingIndex();
  }

  ItemIndex getStartingIndex() {
    return persistency->getStartingIndex();
  }

  struct QueueHandler{
    virtual ~QueueHandler() = default;
    virtual void pushRequest(const std::vector<std::string>& operation) = 0;
  };

  struct SerialQueueHandler : public QueueHandler {
    SerialQueueHandler(BackgroundFlusher * persistency);
    void pushRequest(const std::vector<std::string>& operation) override;
  private:
    BackgroundFlusher * parent;
    QCallback * callback;
    std::mutex newEntriesMtx;
  };
private:
  void itemWasAcknowledged();
  void notifyWaiters();
  std::unique_ptr<BackgroundFlusherPersistency> persistency;

  std::atomic<int64_t> enqueued {0};
  std::atomic<int64_t> acknowledged {0};

  std::mutex newEntriesMtx;

  std::mutex acknowledgementMtx;
  std::condition_variable acknowledgementCV;
  std::atomic<bool> inShutdown {false};

  class FlusherCallback : public QCallback {
  public:
    FlusherCallback(BackgroundFlusher *parent);
    virtual ~FlusherCallback() {}
    virtual void handleResponse(redisReplyPtr &&reply) override;

  private:
    BackgroundFlusher *parent;
  };

  Members members;
  FlusherCallback callback;

  Options options;
  std::unique_ptr<QClient> qclient;
  Notifier &notifier;
  std::unique_ptr<QueueHandler> qhandler;
};

}

#endif
