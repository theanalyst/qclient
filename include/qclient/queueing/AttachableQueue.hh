//------------------------------------------------------------------------------

// File: AttachableQueue.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef QCLIENT_ATTACHABLE_QUEUE_HH
#define QCLIENT_ATTACHABLE_QUEUE_HH

#include "qclient/queueing/WaitableQueue.hh"
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace qclient {

//------------------------------------------------------------------------------
// A queue with similar functionality to a WaitableQueue, but with the ability
// to operate in two distinct modes:
// - Unattached mode, where it behaves exactly like a WaitableQueue.
// - Attached mode, where items are instead forwarded to a specified callback,
//   without queueing at all.
//
// Sometimes we want to queue items, or process them immediatelly as we go.
// Use this class if you want to offer your client the possibility of using
// either.
//------------------------------------------------------------------------------
template<typename T, size_t BlockSize>
class AttachableQueue {
public:
  //----------------------------------------------------------------------------
  // Callback type - function consuming a single T&&
  //----------------------------------------------------------------------------
  using Callback = std::function<void(T&&)>;
  using Iterator = WaitableQueue<T, BlockSize>;

  //----------------------------------------------------------------------------
  // Empty constructor - unattached mode, allocate new queue
  //----------------------------------------------------------------------------
  AttachableQueue() {
    detach();
  }

  //----------------------------------------------------------------------------
  // Start queue directly in attached mode
  //----------------------------------------------------------------------------
  AttachableQueue(Callback &cb) : callback(cb) { }

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  virtual ~AttachableQueue() {}

  //----------------------------------------------------------------------------
  // Construct an item
  //----------------------------------------------------------------------------
  template<typename... Args>
  void emplace_back(Args&&... args) {
    std::lock_guard<std::mutex> lock(mtx);

    if(queue) {
      //------------------------------------------------------------------------
      // Unattached mode
      //------------------------------------------------------------------------
      queue->emplace_back(std::forward<Args>(args)...);
    }
    else {
      //------------------------------------------------------------------------
      // Attached mode, forward to callback
      //------------------------------------------------------------------------
      callback(T(std::forward<Args>(args)...));
    }
  }

  //----------------------------------------------------------------------------
  // Queue accessors - assumes we're running in unattached mode! Will segfault
  // if not..
  //----------------------------------------------------------------------------
  size_t size() const {
    if(!queue) return 0u;
    return queue->size();
  }

  void pop_front() {
    queue->pop_front();
  }

  T& front() {
    return queue->front();
  }

  const T& front() const {
    return queue->front();
  }

  void setBlockingMode(bool value) {
    queue->setBlockingMode(value);
  }

  Iterator begin() {
    return queue->begin();
  }

  int64_t getNextSequenceNumber() const {
    return queue->getNextSequenceNumber();
  }

  //----------------------------------------------------------------------------
  // Attach callback - replace any existing one
  //----------------------------------------------------------------------------
  void attach(const Callback &cb) {
    std::lock_guard<std::mutex> lock(mtx);
    callback = cb;

    if(queue) {
      while(queue->size() != 0u) {
        callback(std::move(queue->front()));
        queue->pop_front();
      }

      queue.reset();
    }
  }

  //----------------------------------------------------------------------------
  // Detach callback - start behaving like a queue again
  //----------------------------------------------------------------------------
  void detach() {
    std::lock_guard<std::mutex> lock(mtx);
    callback = {};

    if(!queue) {
      queue.reset(new WaitableQueue<T, BlockSize>());
    }
  }

private:
  std::mutex mtx;
  std::unique_ptr<WaitableQueue<T, BlockSize>> queue;
  Callback callback;
};

}

#endif

