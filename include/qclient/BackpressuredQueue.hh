//------------------------------------------------------------------------------
// File: BackpressuredQueue.hh
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

#ifndef __QCLIENT_BACKPRESSURED_QUEUE_H__
#define __QCLIENT_BACKPRESSURED_QUEUE_H__

#include <queue>
#include <condition_variable>

namespace qclient {

//------------------------------------------------------------------------------
// A generic multiple-producer, single-consumer queue with built-in back-pressure.
//
// As soon as some configurable cost is reached, the queue can either start
// blocking, or refusing the addition of more items.
//
// The backpressure strategy object receives all push and pop events,
// internally tracking the total costs and deciding when to accept
// new items or not.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// PushStatus: Feedback to the client about the status of their push call.
// ok: whether the item was placed on the queue.
// blockedFor: the amount of time we blocked for.
//------------------------------------------------------------------------------
struct PushStatus {
  bool ok;
  std::chrono::milliseconds blockedFor;
  PushStatus(bool ok_, std::chrono::milliseconds blocked)
  : ok(ok_), blockedFor(blocked) {}
};

//------------------------------------------------------------------------------
// Simplest possible strategy - each item in the queue has a cost of 1, and we
// simply refuse any new items as soon as that limit has been reached.
//------------------------------------------------------------------------------
class BackpressureStrategyLimitSize {
public:
  BackpressureStrategyLimitSize(size_t queueLimit)
  : currentSize(0), limit(queueLimit) {}

  // Notification by the main queue object that an item is about to be added.
  // Returns whether it should be allowed or not.

  template<typename QueueItem>
  bool pushEvent(const QueueItem &item) {
    if(currentSize >= limit) return false;
    currentSize++;
    return true;
  }

  // Notification that an item is about to be removed.
  // Returns whether, after this event, the queue has "space" for more items.

  template<typename QueueItem>
  bool popEvent(const QueueItem &item) {
    currentSize--;
    return true;
  }

private:
  size_t currentSize;
  size_t limit;
};

//------------------------------------------------------------------------------
// The main class.
//------------------------------------------------------------------------------
template<typename QueueItem, typename BackpressureStrategy>
class BackpressuredQueue {
public:
  using container = std::list<QueueItem>;
  using const_iterator = typename container::const_iterator;

  template<typename... Args>
  BackpressuredQueue(Args&&... args) : strategy(std::forward<Args>(args)...) {}

  // Access the top element, assume it exists.
  QueueItem& top() {
    std::unique_lock<std::mutex> lock(mtx);
    return contents.front();
  }

  // Pop an item from the queue, assume it exists.
  void pop() {
    std::unique_lock<std::mutex> lock(mtx);

    contents.pop_front();
    if(strategy.popEvent(contents.front())) {
      waitingToPush.notify_all();
    }
  }

  PushStatus push(const QueueItem &item, std::chrono::milliseconds maxBlockTime = std::chrono::milliseconds::max()) {
    // Keep track of time, in case we block.
    // Initialized lazily to avoid overhead when we don't block.
    std::chrono::steady_clock::time_point blockedSince;
    std::chrono::steady_clock::time_point deadline;
    std::chrono::milliseconds blockedFor(0);

    std::unique_lock<std::mutex> lock(mtx);

    while(true) {
      if(!strategy.pushEvent(item)) {
        goto block;
      }

      // All clear, we can happily place the item on the queue.
      contents.push_back(item);

      // Calculate how long we blocked for, if at all
      if(deadline != std::chrono::steady_clock::time_point()) {
        blockedFor = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - blockedSince);
      }

      waitingToPop.notify_all();
      return PushStatus(true, blockedFor);
block:
      // No block?
      if(maxBlockTime == std::chrono::milliseconds(0)) {
        return PushStatus(false, std::chrono::milliseconds(0));
      }

      // Infinite block?
      if(maxBlockTime == std::chrono::milliseconds::max()) {
        waitingToPush.wait(lock);
        continue;
      }

      // Calculate deadline, if needed
      if(deadline == std::chrono::steady_clock::time_point()) {
        blockedSince = std::chrono::steady_clock::now();
        deadline = blockedSince + maxBlockTime;
      }

      waitingToPush.wait_until(lock, deadline);
      if(std::chrono::steady_clock::now() > deadline) {
        // We've waited past the deadline, refuse request
        blockedFor = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - blockedSince);
        return PushStatus(false, blockedFor);
      }
    }
  }

  size_t size() const {
    std::unique_lock<std::mutex> lock(mtx);
    return contents.size();
  }

  const_iterator begin() const {
    std::unique_lock<std::mutex> lock(mtx);
    return contents.begin();
  }

  const_iterator end() const {
    std::unique_lock<std::mutex> lock(mtx);
    return contents.end();
  }

  // Wait until the queue size is larger than the given value.
  void wait_for(size_t queueSize, std::chrono::milliseconds waitTime) {
    std::unique_lock<std::mutex> lock(mtx);
    if(contents.size() > queueSize) return;
    waitingToPop.wait_for(lock, waitTime);
  }

private:
  BackpressureStrategy strategy;
  std::list<QueueItem> contents;

  mutable std::mutex mtx;
  std::condition_variable waitingToPush;
  std::condition_variable waitingToPop;
};

}

#endif
