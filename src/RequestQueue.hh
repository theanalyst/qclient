//------------------------------------------------------------------------------
// File: RequestQueue.hh
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

#ifndef QCLIENT_REQUEST_QUEUE_HH
#define QCLIENT_REQUEST_QUEUE_HH

#include "qclient/WaitableQueue.hh"
#include "StagedRequest.hh"

namespace qclient {

//------------------------------------------------------------------------------
// A WaitableQueue which holds pending, un-acknowledged requests, with a twist:
// At all times, this class holds one extra, hidden request at the front.
//
// When you do pop_front(), you're not actually destroying the item which you'd
// have gotten with begin(), but the one prior to it.
//
// pop_front() always lags one item behind, but you have no way to actually
// access the hidden element.
//
// Why? The writer event loop may still be accessing the front element, while
// the reader loop has already received a response. We give a leeway of a
// single extra request before deallocating stuff, even after it has been
// satisfied in order to allow the writer loop to progress safely.
//
// EXAMPLE:
// - Writer loop does a ::send with the top request.
// - Response comes very quickly, the reader loop calls pop_front().
// - If we were to free that item now, the writer loop might segfault as it
//   has to access that item again after ::send.
//
// Could there be more responses coming? Well, no, as the writer thread hasn't
// sent those yet.
//
// To prevent the above, we always keep around one extra item at the front of
// the queue, but the interface does not reflect this: Only this class knows
// that this is happening.
//
// begin() will thus return the second item, instead of the first.
//
// To be able to hold this invariant even at startup and keep the code simple,
// we insert a dummy request during construction.
//------------------------------------------------------------------------------

class RequestQueue {
public:
  using QueueType = WaitableQueue<StagedRequest, 5000>;
  using Iterator = QueueType::Iterator;

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  RequestQueue() {
    insertDummyRequest();
  }

  //----------------------------------------------------------------------------
  // Reset queue contents - identical interface to WaitableQueue
  //----------------------------------------------------------------------------
  void reset() {
    queue.reset();
    insertDummyRequest();
  }

  //----------------------------------------------------------------------------
  // Constructs an item inside the queue - identical interface to WaitableQueue
  //----------------------------------------------------------------------------
  template<typename... Args>
  void emplace_back(Args&&... args) {
    queue.emplace_back(std::forward<Args>(args)...);
  }

  //----------------------------------------------------------------------------
  // Pop an item from the front - identical interface to WaitableQueue.
  //----------------------------------------------------------------------------
  void pop_front() {
    queue.pop_front();
  }

  //----------------------------------------------------------------------------
  // Get iterator to the queue - identical interface to WaitableQueue
  //----------------------------------------------------------------------------
  Iterator begin() {
    auto iter = queue.begin();
    iter.next();
    return iter;
  }

  //----------------------------------------------------------------------------
  // Set blocking mode - identical interface to WaitableQueue
  //----------------------------------------------------------------------------
  void setBlockingMode(bool value) {
    queue.setBlockingMode(value);
  }

private:
  void insertDummyRequest() {
    queue.emplace_back(nullptr, EncodedRequest(std::vector<std::string>{"dummy"}));
  }

  QueueType queue;
};

}

#endif
