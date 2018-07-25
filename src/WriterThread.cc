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

#include <poll.h>
#include "WriterThread.hh"
#include "qclient/Handshake.hh"

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

using namespace qclient;

WriterThread::WriterThread(BackpressureStrategy backpressureStr, EventFD &shutdownFD)
: requestStager(backpressureStr), shutdownEventFD(shutdownFD) {
}

WriterThread::~WriterThread() {
  deactivate();
}

void WriterThread::activate(NetworkStream *stream) {
  inHandshake = (handshake.get() != nullptr);
  requestStager.setBlockingMode(true);
  thread.reset(&WriterThread::eventLoop, this, stream);
}

void WriterThread::deactivate() {
  thread.stop();

  std::unique_lock<std::mutex> lock(handshakeMtx);
  handshakeCV.notify_one();
  lock.unlock();

  requestStager.setBlockingMode(false);
  thread.join();

  // Clear handshake
  handshake.reset();
  inHandshake = true;
}

void WriterThread::eventLoop(NetworkStream *networkStream, ThreadAssistant &assistant) {

  struct pollfd polls[2];
  polls[0].fd = shutdownEventFD.getFD();
  polls[0].events = POLLIN;
  polls[1].fd = networkStream->getFd();
  polls[1].events = POLLOUT;

  std::unique_ptr<StagedRequest> localHandshake;
  auto stagingFrontier = requestStager.getIterator();
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
        std::cerr << "qclient: error during poll() in WriterThread::eventLoop: " << errno << ", "
                  << strerror(errno) << std::endl;
      }

      canWrite = true; // try writing again, regardless of poll outcome
    }

    // Determine what exactly we should be writing into the socket.
    if(beingProcessed == nullptr) {

      if(inHandshake) {
        // We're inside a handshake, forbidden to process stagedRequests.
        std::unique_lock<std::mutex> lock(handshakeMtx);

        if(!handshake) {
          // We're supposed to be doing a handshake, but no handshake is available,
          // sleep until it's provided.
          if(!assistant.terminationRequested()) {
            handshakeCV.wait_for(lock, std::chrono::milliseconds(10));
          }
          continue;
        }

        localHandshake = std::move(handshake);
        beingProcessed = localHandshake.get();
        bytesWritten = 0;
      }
      else if(stagingFrontier.itemHasArrived()) {
        // We have requests to process.
        beingProcessed = &stagingFrontier.item();
        stagingFrontier.next();
        bytesWritten = 0;
      }
      else {
        // There are no requests pending to be written, block until there are.
        stagingFrontier.blockUntilItemHasArrived();
        continue;
      }
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
      std::cerr << "qclient: error during send(), return value: " << bytes << ", errno: " << errno << ", "
                << strerror(errno) << std::endl;
      networkStream->shutdown();

      // Stop the loop. The parent class will activate us again with a
      // new network stream if need be.
      return;
    }

    // Seems good, at least some bytes were written. Whoo!
    bytesWritten += bytes;
    if(bytesWritten > beingProcessed->getLen()) {
      std::cerr << "qclient: Something is seriously wrong, wrote more bytes for a request "
                    "than its length: " << bytesWritten << ", " << beingProcessed->getLen() << std::endl;
      exit(1);
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

std::future<redisReplyPtr> WriterThread::stage(EncodedRequest &&req, bool bypassBackpressure) {
  return requestStager.stage(std::move(req), bypassBackpressure);
}

void WriterThread::stage(QCallback *callback, EncodedRequest &&req) {
  requestStager.stage(callback, std::move(req));
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> WriterThread::follyStage(EncodedRequest &&req) {
  return requestStager.follyStage(std::move(req));
}
#endif

void WriterThread::stageHandshake(EncodedRequest &&req) {
  std::lock_guard<std::mutex> lock(handshakeMtx);

  if(!inHandshake) {
    std::cerr << "qclient: bug, attempted to call stageHandshake while inHandshake is false" << std::endl;
    exit(1);
  }

  if(handshake) {
    std::cerr << "qclient: bug, attempted to call stageHandshake while handshake already exists" << std::endl;
    exit(1);
  }

  handshake.reset(new StagedRequest(nullptr, std::move(req)));
  handshakeCV.notify_one();
}

void WriterThread::handshakeCompleted() {
  std::lock_guard<std::mutex> lock(handshakeMtx);
  inHandshake = false;
  handshakeCV.notify_one();
}

void WriterThread::satisfy(redisReplyPtr &&reply) {
  return requestStager.satisfy(std::move(reply));
}

void WriterThread::clearPending() {
  requestStager.clearAllPending();
}
