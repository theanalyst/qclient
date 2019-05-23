//------------------------------------------------------------------------------
// File: WriterThread.cc
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

#include "WriterThread.hh"
#include "ConnectionCore.hh"
#include "network/NetworkStream.hh"
#include "qclient/Handshake.hh"
#include "qclient/Logger.hh"
#include <poll.h>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

using namespace qclient;

WriterThread::WriterThread(Logger *log, ConnectionCore &core, EventFD &shutdownFD)
: logger(log), connectionCore(core), shutdownEventFD(shutdownFD) { }

WriterThread::~WriterThread() {
  deactivate();
}

void WriterThread::activate(NetworkStream *stream) {
  connectionCore.setBlockingMode(true);
  thread.reset(&WriterThread::eventLoop, this, stream);
}

void WriterThread::deactivate() {
  thread.stop();
  connectionCore.setBlockingMode(false);
  thread.join();
}

void WriterThread::eventLoop(NetworkStream *networkStream, ThreadAssistant &assistant) {

  struct pollfd polls[2];
  polls[0].fd = shutdownEventFD.getFD();
  polls[0].events = POLLIN;
  polls[1].fd = networkStream->getFd();
  polls[1].events = POLLOUT;

  StagedRequest *beingProcessed = nullptr;
  size_t bytesWritten = 0;
  bool canWrite = true;

  while(!assistant.terminationRequested() && networkStream->ok()) {
    // What should we do during this round?

    if(!canWrite) {
      // We have data to write but cannot, because the kernel buffers are full.
      // Poll until the socket is writable.

      int rpoll = poll(polls, 2, -1);
      if(rpoll < 0 && errno != EINTR) {
        QCLIENT_LOG(logger, LogLevel::kError,
          "error during poll() in WriterThread::eventLoop. errno="
          << errno << ":" << strerror(errno));
      }

      canWrite = true; // try writing again, regardless of poll outcome
    }

    // Determine what exactly we should be writing into the socket. getNextToWrite
    // will block until there's something to write, or shutdown has been requested.
    if(beingProcessed == nullptr) {
      bytesWritten = 0;
      beingProcessed = connectionCore.getNextToWrite();
      if(!beingProcessed) continue;
    }

    // The socket is writable AND there's staged requests waiting to be written.
    int bytes;

    bytes = networkStream->send(
      beingProcessed->getBuffer() + bytesWritten,
      beingProcessed->getLen() - bytesWritten
    );

    // Determine what happened during sending.
    if(bytes < 0 && errno == EWOULDBLOCK) {
      // Recoverable error: EWOULDBLOCK
      // All is good, we just need to poll before writing again.
      canWrite = false;
      continue;
    }

    if(bytes < 0) {
      // Non-recoverable error, this looks bad. Kill connection.
      QCLIENT_LOG(logger, LogLevel::kError, "Bad return value from send(): "
        << bytes << ", errno: " << errno << "," << strerror(errno));
      networkStream->shutdown();

      // Stop the loop. The parent class will activate us again with a
      // new network stream if need be.
      return;
    }

    // Seems good, at least some bytes were written. Whoo!
    bytesWritten += bytes;
    if(bytesWritten > beingProcessed->getLen()) {
      QCLIENT_LOG(logger, LogLevel::kFatal, "Wrote more bytes for a request than its length: "
        << bytesWritten << ", " << beingProcessed->getLen());
      std::abort();
    }

    // Are we done with 'beingProcessed' yet?
    if(bytesWritten == beingProcessed->getLen()) {
      // Yep, set to null and process next one.
      beingProcessed = nullptr;
    }
    else {
      // Fewer bytes were written than the full length of the request, the
      // kernel buffers must be full. Poll until the socket is writable.
      canWrite = false;
    }
  }
}
