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

CallbackExecutorThread::CallbackExecutorThread()
: thread(&CallbackExecutorThread::main, this) {}

CallbackExecutorThread::~CallbackExecutorThread() {
  thread.stop();
  pendingCallbacks.setBlockingMode(false);
  thread.join();
}

void CallbackExecutorThread::main(ThreadAssistant &assistant) {
  auto frontier = pendingCallbacks.begin();

  while(true) {
    if(assistant.terminationRequested() && !frontier.itemHasArrived()) {
      //------------------------------------------------------------------------
      // Even if termination is requested, we don't quit until all callbacks
      // have been serviced! We don't want any hanging futures, for example.
      //------------------------------------------------------------------------
      break;
    }

    PendingCallback *cb = frontier.getItemBlockOrNull();
    if(!cb) continue;
    cb->callback->handleResponse(std::move(cb->reply));

    frontier.next();
    pendingCallbacks.pop_front();
  }
}

void CallbackExecutorThread::stage(QCallback *callback, redisReplyPtr &&response) {
  pendingCallbacks.emplace_back(callback, std::move(response));
}
