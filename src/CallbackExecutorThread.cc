//------------------------------------------------------------------------------
// File: CallbackExecutorThread.cc
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

#include "CallbackExecutorThread.hh"

using namespace qclient;

CallbackExecutorThread::CallbackExecutorThread() {
  pendingCallbacks = &callbackStore1;

  thread.reset(&CallbackExecutorThread::main, this);
}

CallbackExecutorThread::~CallbackExecutorThread() {
  thread.stop();
  std::unique_lock<std::mutex> lock(mtx);
  cv.notify_one();
  lock.unlock();
  thread.join();
}

std::deque<PendingCallback>& CallbackExecutorThread::swapStoresAndReturnOld() {
  if(pendingCallbacks == &callbackStore1) {
    pendingCallbacks = &callbackStore2;
    return callbackStore1;
  }

  pendingCallbacks = &callbackStore1;
  return callbackStore2;
}

void CallbackExecutorThread::main(ThreadAssistant &assistant) {
  std::unique_lock<std::mutex> lock(mtx);
  while(true) {
    if(assistant.terminationRequested() && pendingCallbacks->size() == 0) {
      break;
    }

    if(pendingCallbacks->size() == 0) {
      // Empty queue, sleep
      cv.wait_for(lock, std::chrono::seconds(120));
      continue;
    }

    // Swap the stores under lock
    std::deque<PendingCallback> &callbacksToExec = swapStoresAndReturnOld();

    // Unblock staging
    lock.unlock();

    // Execute callbacks, while stagers are able to push more
    for(size_t i = 0; i < callbacksToExec.size(); i++) {
      PendingCallback &cb = callbacksToExec[i];
      cb.callback->handleResponse(std::move(cb.reply));
    }

    callbacksToExec.clear();
    lock.lock();
  }
}

void CallbackExecutorThread::stage(QCallback *callback, redisReplyPtr &&response) {
  std::lock_guard<std::mutex> lock(mtx);
  pendingCallbacks->emplace_back(callback, std::move(response));
  cv.notify_one();
}
