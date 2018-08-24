//------------------------------------------------------------------------------
// File: WriterThread.hh
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

#ifndef QCLIENT_WRITER_THREAD_H
#define QCLIENT_WRITER_THREAD_H

#include <hiredis/hiredis.h>
#include "qclient/AssistedThread.hh"
#include "qclient/EventFD.hh"
#include "FutureHandler.hh"
#include "NetworkStream.hh"
#include "BackpressureApplier.hh"
#include "CallbackExecutorThread.hh"
#include "StagedRequest.hh"
#include "RequestStager.hh"
#include "ConnectionHandler.hh"
#include "qclient/ThreadSafeQueue.hh"
#include "qclient/Options.hh"
#include "qclient/EncodedRequest.hh"
#include <deque>
#include <future>

namespace qclient {

using redisReplyPtr = std::shared_ptr<redisReply>;

class WriterThread {
public:
  WriterThread(Logger *logger, ConnectionHandler &handler, EventFD &shutdownFD);
  ~WriterThread();

  void activate(NetworkStream *stream);
  void deactivate();
  void eventLoop(NetworkStream *stream, ThreadAssistant &assistant);

private:
  Logger *logger;
  ConnectionHandler &connectionHandler;
  EventFD &shutdownEventFD;
  AssistedThread thread;

};

}

#endif
