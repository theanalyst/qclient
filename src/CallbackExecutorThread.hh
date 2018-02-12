//------------------------------------------------------------------------------
// File: CallbackExecutorThread.hh
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

#ifndef QCLIENT_CALLBACK_EXECUTOR_H
#define QCLIENT_CALLBACK_EXECUTOR_H

#include <string>
#include <atomic>
#include "qclient/QCallback.hh"
#include "qclient/AssistedThread.hh"

namespace qclient {

struct PendingCallback {
  PendingCallback(QCallback *cb, redisReplyPtr &&rep) : callback(cb),
  reply(std::move(rep)) {}

  QCallback *callback;
  redisReplyPtr reply;
};

class CallbackExecutorThread {
public:
  CallbackExecutorThread();
  ~CallbackExecutorThread();

  void main(ThreadAssistant &assistant);
  void stage(QCallback *callback, redisReplyPtr &&reply);

private:
  AssistedThread thread;

  std::deque<PendingCallback> callbackStore1;
  std::deque<PendingCallback> callbackStore2;

  std::deque<PendingCallback> *pendingCallbacks;
  std::mutex mtx;
  std::condition_variable cv;

  std::deque<PendingCallback>& swapStoresAndReturnOld();
};

}

#endif
