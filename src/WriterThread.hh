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

#include "qclient/AssistedThread.hh"
#include "qclient/EventFD.hh"
#include "FutureHandler.hh"
#include "BackpressureApplier.hh"
#include "CallbackExecutorThread.hh"
#include "StagedRequest.hh"
#include "qclient/Options.hh"
#include "qclient/EncodedRequest.hh"
#include <deque>
#include <future>

namespace qclient {

class ConnectionCore;
class NetworkStream;

class WriterThread {
public:
  WriterThread(Logger *logger, ConnectionCore &core, EventFD &shutdownFD);
  ~WriterThread();

  void activate(NetworkStream *stream);
  void deactivate();
  void eventLoop(NetworkStream *stream, ThreadAssistant &assistant);

private:
  Logger *logger;
  ConnectionCore &connectionCore;
  EventFD &shutdownEventFD;
  AssistedThread thread;
};

}

#endif
