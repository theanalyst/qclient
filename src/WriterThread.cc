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

using namespace qclient;

WriterThread::WriterThread(EventFD &shutdownFD) : shutdownEventFD(shutdownFD) {
}

WriterThread::~WriterThread() {
  deactivate();
  clearPending();
}

void WriterThread::activate(NetworkStream *stream) {
  nextToFlush = 0;
  nextToAcknowledge = 0;
  thread.reset(&WriterThread::eventLoop, this, stream);
}

void WriterThread::deactivate() {
  thread.stop();

  std::unique_lock<std::mutex> lock(stagingMtx);
  stagingCV.notify_one();
  lock.unlock();

  thread.join();

  // Clear all acknowledged without leeway.
  clearAcknowledged(0);

  // We'll need to send again the first few "unacknowledged but flushed" items.
  nextToFlush = 0;
}

void WriterThread::clearAcknowledged(size_t leeway) {
  // Clear acknowledged requests, but keeping a leeway of N top items.
  // There's a race right after ::send - the response might come at any time,
  // even though eventLoop is not done yet with this item. Removing it would
  // cause bad things to happen, so we keep a leeway of a acknowledged items.
  // (Only the top one is needed to keep, but let's be conservative)

  // Assumption: It's safe to make changes to stagedRequests.
  // stagingMtx is either locked, or only one thread is accessing it.

  while(nextToAcknowledge > (int) leeway) {
    nextToFlush--;
    nextToAcknowledge--;

    stagedRequests.pop_front();
  }
}

void WriterThread::clearPending() {
  std::lock_guard<std::mutex> lock(stagingMtx);

  for(size_t i = nextToAcknowledge; i < stagedRequests.size(); i++) {
    stagedRequests[i].set_value(redisReplyPtr());
  }

  nextToFlush = 0;
  nextToAcknowledge = 0;
  stagedRequests.clear();
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
      // Poll until the socket is writable. Prevent addition of further requests
      // as a primitive form of back-pressure.

      std::lock_guard<std::mutex> lock(appendMtx);

      int rpoll = poll(polls, 2, -1);
      if(rpoll < 0 && errno != EINTR) {
        std::cerr << "qclient: error during poll() in WriterThread::eventLoop: " << errno << ", "
                  << strerror(errno) << std::endl;
      }

      canWrite = true; // try writing again, regardless of poll outcome
    }

    // Determine what exactly we should be writing into the socket.
    if(beingProcessed == nullptr) {
      std::unique_lock<std::mutex> lock(stagingMtx);

      if(nextToFlush < (int) stagedRequests.size()) {
        beingProcessed = &stagedRequests.at(nextToFlush);
        nextToFlush++;
        bytesWritten = 0;
      }
      else {
        // Nope, no requests are pending, just sleep.
        if(assistant.terminationRequested()) continue;
        stagingCV.wait_for(lock, std::chrono::seconds(1));
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

    // Fewer bytes were written than the full length of the request, the
    // kernel buffers must be full. Poll until the socket is writable.
    canWrite = false;
  }

}

std::future<redisReplyPtr> WriterThread::stage(char *buffer, size_t len) {
  std::lock_guard<std::mutex> lock2(appendMtx);
  std::lock_guard<std::mutex> lock(stagingMtx);

  stagedRequests.emplace_back(std::move(buffer), len);
  stagingCV.notify_one();
  return stagedRequests.back().get_future();
}

void WriterThread::satisfy(redisReplyPtr &reply) {
  std::lock_guard<std::mutex> lock(stagingMtx);

  stagedRequests[nextToAcknowledge].set_value(reply);
  nextToAcknowledge++;
  clearAcknowledged(3);
}
