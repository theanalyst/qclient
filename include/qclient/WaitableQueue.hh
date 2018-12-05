//------------------------------------------------------------------------------
// File: WaitableQueue.hh
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

#ifndef QCLIENT_WAITABLE_QUEUE_H
#define QCLIENT_WAITABLE_QUEUE_H

#include "qclient/ThreadSafeQueue.hh"
#include <atomic>
#include <mutex>
#include <condition_variable>

namespace qclient {

//------------------------------------------------------------------------------
// A class which builds on ThreadSafeQueue and offers a more comfortable API
// in the common case of:
// - A single thread consuming items.
// - Multiple threads producing them.
//
// Abstracts away the queue sequence numbers. This functionality is not part
// of the ThreadSafeQueue, as it's more heavyweight, having to deal with
// atomics and condition variables.
//
// Semantics are still the same: Readers almost never block each other (only
// obtainers of the long-lived iterator block each other), and writers only
// block other writers.
//------------------------------------------------------------------------------

template<typename T, size_t BlockSize>
class WaitableQueue {
public:
  WaitableQueue() { }

  void reset() {
    highestSequence = -1;
    queue.reset();
  }

  //----------------------------------------------------------------------------
  // Constructs an item inside the queue.
  //----------------------------------------------------------------------------
  template<typename... Args>
  void emplace_back(Args&&... args) {
    std::lock_guard<std::mutex> lock(mtx);
    highestSequence = queue.emplace_back(std::forward<Args>(args)...);
    cv.notify_one();
  }

  //----------------------------------------------------------------------------
  // Check size of the queue
  //----------------------------------------------------------------------------
  size_t size() const {
    return queue.size();
  }

  //----------------------------------------------------------------------------
  // Pop an item from the front.
  //----------------------------------------------------------------------------
  void pop_front() {
    queue.pop_front();
  }

  //----------------------------------------------------------------------------
  // If blocking mode is set to false, threads in blockUntilItemHasArrived
  // do not realy block.
  //
  // Set it to false whenever you want to unblock threads waiting inside
  // that function, such as during shutdown.
  //
  // Set it to true before activating some event loop that has to wait on
  // incoming items.
  //----------------------------------------------------------------------------
  void setBlockingMode(bool value) {
    std::unique_lock<std::mutex> lock(mtx);
    blockingMode = value;
    cv.notify_one();
  }

  class Iterator {
  public:
    Iterator() : queue(nullptr) {}

    Iterator(WaitableQueue<T, BlockSize> *q) : queue(q), iterator(queue->queue.begin()) {}

    T& item() {
      return iterator.item();
    }

    const T& item() const {
      return iterator.item();
    }

    void next() {
      iterator.next();
    }

    //--------------------------------------------------------------------------
    // If false, the item we're pointing to is past the edge of the queue,
    // and has not arrived yet. It would be unsafe to call item() and next().
    //
    // The iterator object is still valid, though. As soon as the item arrives,
    // calling item() is perfectly OK.
    //--------------------------------------------------------------------------
    bool itemHasArrived() {
      return queue->highestSequence >= iterator.seq();
    }

    //--------------------------------------------------------------------------
    // After getting false from itemHasArrived, call this to sleep until there's
    // some item to process.
    //
    // NOTE: It's not safe to assume itemHasArrived == true after returning,
    // since it could be that blocking mode was set to false. You have to call
    // itemHasArrived again.
    //--------------------------------------------------------------------------
    void blockUntilItemHasArrived() {
      std::unique_lock<std::mutex> lock(queue->mtx);

      while(queue->blockingMode && iterator.seq() > queue->highestSequence) {
        queue->cv.wait(lock);
      }
    }

    //--------------------------------------------------------------------------
    // Fetch the next item: If none exists, block. If none exists and blocking
    // mode is disabled, return nullptr.
    //--------------------------------------------------------------------------
    T* getItemBlockOrNull() {
      if(itemHasArrived()) {
        return &item();
      }

      blockUntilItemHasArrived();
      if(!itemHasArrived()) {
        //----------------------------------------------------------------------
        // We managed to escape blockUntilItem<hasArrived(), even though no
        // item is available. Can only mean one thing: Blocking mode is off,
        // return nullptr.
        //----------------------------------------------------------------------
        return nullptr;
      }

      return &item();
    }

  private:
    WaitableQueue<T, BlockSize> *queue;
    typename ThreadSafeQueue<T, BlockSize>::Iterator iterator;
  };

  //----------------------------------------------------------------------------
  // Get iterator to the queue
  //----------------------------------------------------------------------------
  Iterator begin() {
    return Iterator(this);
  }

private:
  friend class WaitableQueue<T, BlockSize>::Iterator;
  ThreadSafeQueue<T, BlockSize> queue;
  std::atomic<int64_t> highestSequence {-1};
  std::mutex mtx;
  std::condition_variable cv;
  std::atomic<bool> blockingMode {true};
};

}

#endif
