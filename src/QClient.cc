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

// Instantiate std::future<redisReplyPtr> to save compile time
template class std::future<qclient::redisReplyPtr>;

using namespace qclient;
#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl;

//------------------------------------------------------------------------------
// The intercepts machinery
//------------------------------------------------------------------------------
std::mutex QClient::interceptsMutex;
std::map<std::pair<std::string, int>, std::pair<std::string, int>>
    QClient::intercepts;


void QClient::addIntercept(const std::string& hostname, int port,
                           const std::string& host2, int port2)
{
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts[std::make_pair(hostname, port)] = std::make_pair(host2, port2);
}

void QClient::clearIntercepts()
{
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts.clear();
}

//------------------------------------------------------------------------------
// Constructor taking host and port
//-----------------------------------------------------------------------------
QClient::QClient(const std::string& host_, const int port_, bool redirects,
                 RetryStrategy retries, TlsConfig tlc, std::unique_ptr<Handshake> handshake_)
  : members(host_, port_), transparentRedirects(redirects),
    retryStrategy(retries), tlsconfig(tlc), handshake(std::move(handshake_))
{
  startEventLoop();
}


//------------------------------------------------------------------------------
// Constructor taking list of members for the cluster
//------------------------------------------------------------------------------
QClient::QClient(const Members& members_, bool redirects,
                 RetryStrategy retries, TlsConfig tlc, std::unique_ptr<Handshake> handshake_)
  : members(members_), transparentRedirects(redirects),
    retryStrategy(retries), tlsconfig(tlc), handshake(std::move(handshake_))
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
void QClient::execute(QCallback *callback, char *buffer, const size_t len) {
  std::unique_lock<std::recursive_mutex> lock(mtx);
  writerThread->stage(callback, buffer, len);
}

std::future<redisReplyPtr> QClient::execute(char *buffer, const size_t len) {
  std::unique_lock<std::recursive_mutex> lock(mtx);

  std::future<redisReplyPtr> retval = futureHandler.stage();
  writerThread->stage(&futureHandler, buffer, len);
  return retval;
}

//------------------------------------------------------------------------------
// Convenience function to encode a redis command given as an array of char*
// and sizes to a redis buffer
//------------------------------------------------------------------------------
std::future<redisReplyPtr> QClient::execute(size_t nchunks, const char** chunks,
                                            const size_t* sizes) {
  char* buffer = NULL;
  int len = redisFormatCommandArgv(&buffer, nchunks, chunks, sizes);
  return execute(buffer, len);
}

void QClient::execute(QCallback *callback, size_t nchunks, const char** chunks,
                                            const size_t* sizes) {
  char* buffer = NULL;
  int len = redisFormatCommandArgv(&buffer, nchunks, chunks, sizes);
  execute(callback, buffer, len);
}

void QClient::stageHandshake(const std::vector<std::string> &cont) {
  std::unique_lock<std::recursive_mutex> lock(mtx);

  std::uint64_t size = cont.size();
  std::uint64_t indx = 0;
  const char* cstr[size];
  size_t sizes[size];

  for (auto it = cont.begin(); it != cont.end(); ++it) {
    cstr[indx] = it->data();
    sizes[indx] = it->size();
    ++indx;
  }

  char* buffer = NULL;

  int len = redisFormatCommandArgv(&buffer, size, cstr, sizes);
  writerThread->stageHandshake(buffer, len);
}

//------------------------------------------------------------------------------
// Event loop for the client
//------------------------------------------------------------------------------
void QClient::startEventLoop()
{
  // Give some leeway when starting up before declaring the cluster broken.
  lastAvailable = std::chrono::steady_clock::now();

  writerThread = new WriterThread(shutdownEventFD);
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
      Handshake::Status status = handshake->validateResponse(rr);

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
        stageHandshake(handshake->provideHandshake());
      }

      continue;
    }

    // Is this a redirect?
    if (transparentRedirects && rr->type == REDIS_REPLY_ERROR &&
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
    if(retryStrategy.enabled && rr->type == REDIS_REPLY_ERROR &&
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
  if(!retryStrategy.enabled || lastAvailable + retryStrategy.timeout < std::chrono::steady_clock::now()) {
    writerThread->clearPending();
  }
}

//------------------------------------------------------------------------------
// Set up TCP connection
//------------------------------------------------------------------------------
void QClient::connectTCP()
{
  networkStream = new NetworkStream(targetEndpoint.getHost(),
                                    targetEndpoint.getPort(), tlsconfig);

  if(!networkStream->ok()) {
    return;
  }

  if(handshake) {
    handshake->restart();
    stageHandshake(handshake->provideHandshake());
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
  std::unique_lock<std::recursive_mutex> lock(mtx);
  cleanup();

  targetEndpoint = members.getEndpoints()[nextMember];
  nextMember = (nextMember + 1) % members.size();

  processRedirection();
  discoverIntercept();
  reader = redisReaderCreate();
  connectTCP();
}

void QClient::eventLoop()
{
  const size_t BUFFER_SIZE = 1024 * 2;
  char buffer[BUFFER_SIZE];
  signal(SIGPIPE, SIG_IGN);
  std::chrono::milliseconds backoff(1);

  while (true) {
    bool activeConnection = false;
    std::unique_lock<std::recursive_mutex> lock(mtx);
    struct pollfd polls[2];
    polls[0].fd = shutdownEventFD.getFD();
    polls[0].events = POLLIN;
    polls[1].fd = networkStream->getFd();
    polls[1].events = POLLIN;

    RecvStatus status(true, 0, 0);
    while (networkStream->ok()) {
      lock.unlock();

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
          exec("PING", "qclient-connection-initialization");
          activeConnection = true;
        }
      }
      lock.lock();

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

    lock.unlock();
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

void QClient::discoverIntercept()
{
  // If this (host, port) pair is being intercepted, redirect to a different
  // (host, port) pair instead.
  std::lock_guard<std::mutex> lock(interceptsMutex);
  auto it = intercepts.find(std::make_pair(targetEndpoint.getHost(), targetEndpoint.getPort()));

  if (it != intercepts.end()) {
    targetEndpoint = Endpoint(it->second.first, it->second.second);
  }
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
