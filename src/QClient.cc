// ----------------------------------------------------------------------
// File: QClient.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#include "qclient/QClient.hh"
#include "qclient/Utils.hh"
#include <unistd.h>
#include <string.h>
#include <poll.h>
#include <signal.h>
#include <fcntl.h>
#include <sstream>
#include <iterator>
#include "qclient/ConnectionInitiator.hh"
#include "NetworkStream.hh"
#include "WriterThread.hh"
#include "qclient/GlobalInterceptor.hh"

//------------------------------------------------------------------------------
//! Instantiate a few templates inside this compilation unit, to save compile
//! time. The alternative is to have every single compilation unit which
//! includes QClient.hh instantiate them, which increases compilation time.
//------------------------------------------------------------------------------
template class std::future<qclient::redisReplyPtr>;
#if HAVE_FOLLY == 1
template class folly::Future<qclient::redisReplyPtr>;
#endif

using namespace qclient;
#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl;

//------------------------------------------------------------------------------
// Constructor taking host and port
//-----------------------------------------------------------------------------
QClient::QClient(const std::string& host_, const int port_, Options &&opts)
  : members(host_, port_), options(std::move(opts))
{
  startEventLoop();
}


//------------------------------------------------------------------------------
// Constructor taking list of members for the cluster
//------------------------------------------------------------------------------
QClient::QClient(const Members& members_, Options &&opts)
  : members(members_), options(std::move(opts))
{
  startEventLoop();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QClient::~QClient()
{
  shutdown = true;
  shutdownEventFD.notify();
  eventLoopThread.join();
  cleanup();
  delete writerThread;
}

//------------------------------------------------------------------------------
// Primary execute command that takes a redis encoded buffer and sends it
// over the network
//------------------------------------------------------------------------------
void QClient::execute(QCallback *callback, EncodedRequest &&req) {
  writerThread->stage(callback, std::move(req));
}

std::future<redisReplyPtr> QClient::execute(EncodedRequest &&req) {
  return writerThread->stage(std::move(req));
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> QClient::follyExecute(EncodedRequest &&req) {
  return writerThread->follyStage(std::move(req));
}
#endif

void QClient::stageHandshake(const std::vector<std::string> &cont) {
  writerThread->stageHandshake(EncodedRequest(cont));
}

//------------------------------------------------------------------------------
// Event loop for the client
//------------------------------------------------------------------------------
void QClient::startEventLoop()
{
  // Give some leeway when starting up before declaring the cluster broken.
  lastAvailable = std::chrono::steady_clock::now();

  writerThread = new WriterThread(options.backpressureStrategy, shutdownEventFD);
  connect();
  eventLoopThread = std::thread(&QClient::eventLoop, this);
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool QClient::feed(const char* buf, size_t len)
{
  if (len > 0) {
    redisReaderFeed(reader, buf, len);
  }

  while (true) {
    void* reply = NULL;

    if (redisReaderGetReply(reader, &reply) == REDIS_ERR) {
      return false;
    }

    if (reply == NULL) {
      break;
    }

    // We have a new response from the server.
    redisReplyPtr rr = redisReplyPtr(redisReplyPtr((redisReply*) reply, freeReplyObject));

    // Is this a response to the handshake?
    if(handshakePending) {
      Handshake::Status status = options.handshake->validateResponse(rr);

      if(status == Handshake::Status::INVALID) {
        // Error during handshaking, drop connection
        return false;
      }

      successfulResponses = true;
      if(status == Handshake::Status::VALID_COMPLETE) {
        // We're done handshaking
        handshakePending = false;
        writerThread->handshakeCompleted();
      }

      if(status == Handshake::Status::VALID_INCOMPLETE) {
        // Still more requests to go
        stageHandshake(options.handshake->provideHandshake());
      }

      continue;
    }

    // Is this a redirect?
    if (options.transparentRedirects && rr->type == REDIS_REPLY_ERROR &&
        strncmp(rr->str, "MOVED ", strlen("MOVED ")) == 0) {

      std::vector<std::string> response = split(std::string(rr->str, rr->len), " ");
      RedisServer redirect;

      if (response.size() == 3 && parseServer(response[2], redirect)) {
        redirectedEndpoint = Endpoint(redirect.host, redirect.port);
        return false;
      }
    }

    // Is this a transient "unavailable" error?
    // Checking for "ERR unavailable" is a QDB specific hack! We should introduce
    // a new response type, ie "UNAVAILABLE reason for unavailability"
    if(options.retryStrategy.active() && rr->type == REDIS_REPLY_ERROR &&
       strncmp(rr->str, "ERR unavailable", strlen("ERR unavailable")) == 0) {

      // Break connection, try again.
      return false;
    }

    // We're all good, satisfy request.
    successfulResponses = true;
    writerThread->satisfy(std::move(rr));
  }

  return true;
}

//----------------------------------------------------------------------------
// Should we purge any requests currently queued inside WriterThread?
//----------------------------------------------------------------------------
bool QClient::shouldPurgePendingRequests() {
  if(options.retryStrategy.getMode() == RetryStrategy::Mode::kInfiniteRetries) {
    //--------------------------------------------------------------------------
    // Infinite retries, nope.
    //--------------------------------------------------------------------------
    return false;
  }

  if(options.retryStrategy.getMode() == RetryStrategy::Mode::kRetryWithTimeout &&
     lastAvailable + options.retryStrategy.getTimeout() >= std::chrono::steady_clock::now()) {
    //--------------------------------------------------------------------------
    // Timeout has not expired yet, nope.
    //--------------------------------------------------------------------------
    return false;
  }

  //--------------------------------------------------------------------------
  // Yes, purge.
  //--------------------------------------------------------------------------
  return true;
}

//------------------------------------------------------------------------------
// Cleanup before reconnection or when exiting
//------------------------------------------------------------------------------
void QClient::cleanup()
{
  writerThread->deactivate();

  if(networkStream) {
    delete networkStream;
  }

  networkStream = nullptr;

  if (reader != nullptr) {
    redisReaderFree(reader);
    reader = nullptr;
  }

  successfulResponses = false;

  if(shouldPurgePendingRequests()) {
    writerThread->clearPending();
  }
}

//------------------------------------------------------------------------------
// Set up TCP connection
//------------------------------------------------------------------------------
void QClient::connectTCP()
{
  networkStream = new NetworkStream(targetEndpoint.getHost(),
                                    targetEndpoint.getPort(), options.tlsconfig);

  if(!networkStream->ok()) {
    return;
  }

  if(options.handshake) {
    options.handshake->restart();
    stageHandshake(options.handshake->provideHandshake());
    handshakePending = true;
  }
  else {
    writerThread->handshakeCompleted();
    handshakePending = false;
  }

  writerThread->activate(networkStream);

}

//------------------------------------------------------------------------------
// Connect
//------------------------------------------------------------------------------
void QClient::connect()
{
  cleanup();

  targetEndpoint = members.getEndpoints()[nextMember];
  nextMember = (nextMember + 1) % members.size();

  processRedirection();
  reader = redisReaderCreate();

  targetEndpoint = GlobalInterceptor::translate(targetEndpoint);
  connectTCP();
}

//------------------------------------------------------------------------------
// Prime connection with a dummy request to prevent timeout
//------------------------------------------------------------------------------
void QClient::primeConnection() {
  // Important: We must bypass backpressure, otherwise we risk deadlocking the
  // main event loop.

  std::vector<std::string> req { "PING", "qclient-connection-initialization" };
  writerThread->stage(EncodedRequest(req), true /* bypass backpressure */ );
}

void QClient::eventLoop()
{
  const size_t BUFFER_SIZE = 1024 * 2;
  char buffer[BUFFER_SIZE];
  signal(SIGPIPE, SIG_IGN);
  std::chrono::milliseconds backoff(1);

  while (true) {
    bool activeConnection = false;
    struct pollfd polls[2];
    polls[0].fd = shutdownEventFD.getFD();
    polls[0].events = POLLIN;
    polls[1].fd = networkStream->getFd();
    polls[1].events = POLLIN;

    RecvStatus status(true, 0, 0);
    while (networkStream->ok()) {
      // If the previous iteration returned any bytes at all, try to read again
      // without polling. It could be that there's more data cached inside
      // OpenSSL, which poll() will not detect.

      if(status.bytesRead <= 0) {
        int rpoll = poll(polls, 2, 60);
        if(rpoll < 0 && errno != EINTR) {
          // something's wrong, try to reconnect
          break;
        }

        if(rpoll == 0 && !activeConnection) {
          // No bytes have been transfered on this link, send a dummy
          // reqeust to prevent a timeout.
          primeConnection();
          activeConnection = true;
        }
      }

      if (shutdown) {
        break;
      }

      // legit connection, reset backoff
      backoff = std::chrono::milliseconds(1);
      status = networkStream->recv(buffer, BUFFER_SIZE, 0);

      if(!status.connectionAlive) {
        break; // connection died on us, try to reconnect
      }

      if(status.bytesRead > 0) {
        activeConnection = true;
      }

      if(status.bytesRead > 0 && !feed(buffer, status.bytesRead)) {
        break; // protocol violation
      }
    }

    if (shutdown) {
      feed(NULL, 0);
      break;
    }

    std::this_thread::sleep_for(backoff);

    // Give some more leeway, update lastAvailable after sleeping.
    if(successfulResponses) {
      lastAvailable = std::chrono::steady_clock::now();
    }

    if (backoff < std::chrono::milliseconds(2048)) {
      backoff++;
    }

    this->connect();
  }
}

void QClient::processRedirection()
{
  if (!redirectedEndpoint.empty()) {
    std::cerr << "qclient: redirecting to " << redirectedEndpoint.toString() << std::endl;
    targetEndpoint = redirectedEndpoint;
    redirectionActive = true;
  } else if (redirectionActive) {
    std::cerr << "qclient: redirecting back to original hosts " << std::endl;
    redirectionActive = false;
  }

  redirectedEndpoint = {};
}

//------------------------------------------------------------------------------
// Wrapper function for exists command
//------------------------------------------------------------------------------
long long int
QClient::exists(const std::string& key)
{
  redisReplyPtr reply = exec("EXISTS", key).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error exists key: " + key +
                             ": Unexpected/null reply ");
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Wrapper function for del async command
//------------------------------------------------------------------------------
std::future<redisReplyPtr>
QClient::del_async(const std::string& key)
{
  return exec("DEL", key);
}

//------------------------------------------------------------------------------
// Wrapper function for del command
//------------------------------------------------------------------------------
long long int
QClient::del(const std::string& key)
{
  redisReplyPtr reply = exec("DEL", key).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error del key: " + key +
                             ": Unexpected/null reply ");
  }

  return reply->integer;
}
