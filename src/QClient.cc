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
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <sstream>
#include <iterator>

#include "ConnectionInitiator.hh"
#include "NetworkStream.hh"

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
// QClient class implementation
//------------------------------------------------------------------------------
QClient::QClient(const std::string& host_, const int port_, bool redirects,
                 bool exceptions, TlsConfig tlc, std::vector<std::string> handshake)
  : members(host_, port_), transparentRedirects(redirects),
    exceptionsEnabled(exceptions), tlsconfig(tlc), sock(-1), handshakeCommand(handshake)
{
  startEventLoop();
}

QClient::QClient(const Members& members_, bool redirects,
                 bool exceptions, TlsConfig tlc, std::vector<std::string> handshake)
  : members(members_), transparentRedirects(redirects), exceptionsEnabled(exceptions),
    tlsconfig(tlc), sock(-1), handshakeCommand(handshake)
{
  startEventLoop();
}

QClient::~QClient()
{
  shutdown = true;
  shutdownEventFD.notify();
  eventLoopThread.join();
  cleanup();
}

void QClient::blockUntilWritable() {
  struct pollfd polls[2];
  polls[0].fd = shutdownEventFD.getFD();
  polls[0].events = POLLIN;
  polls[1].fd = sock;
  polls[1].events = POLLOUT;

  int rpoll = poll(polls, 2, -1);
  if(rpoll < 0 && errno != EINTR) {
    std::cerr << "qclient: error during poll() in blockUntilWritable: " << errno << ", "
              << strerror(errno) << std::endl;
  }
}

std::future<redisReplyPtr> QClient::execute(const char* buffer,
                                            const size_t len)
{
  std::lock_guard<std::recursive_mutex> lock(mtx);

  // not connected temporarily?
  if (sock <= 0) {
    std::promise<redisReplyPtr> prom;

    // not available at all?
    if(exceptionsEnabled && !available) {
      prom.set_exception(std::make_exception_ptr(std::runtime_error("unavailable")));
    }
    else {
      prom.set_value(redisReplyPtr());
    }

    return prom.get_future();
  }

  int bytes;
  bytes = networkStream->send(buffer, len);

  if(bytes != (int) len) {
    // Recoverable error: EWOULDBLOCK
    if(bytes < 0 && errno == EWOULDBLOCK) {
      blockUntilWritable();
      return execute(buffer, len);
    }

    // Recoverable error: Fewer bytes were written than anticipated
    if(bytes > 0 && bytes < (int) len) {
      blockUntilWritable();
      return execute(buffer+bytes, len-bytes);
    }

    // Handle non-recoverable errors, kill connection
    std::cerr << "qclient: error during send(), return value: " << bytes << ", errno: " << errno << ", "
              << strerror(errno) << std::endl;
    networkStream->shutdown();

    std::promise<redisReplyPtr> prom;
    prom.set_value(redisReplyPtr());
    return prom.get_future();
  }

  promises.emplace();
  return promises.back().get_future();
}

std::future<redisReplyPtr> QClient::execute(size_t nchunks, const char** chunks,
                                            const size_t* sizes)
{
  char* buffer = NULL;
  int len = redisFormatCommandArgv(&buffer, nchunks, chunks, sizes);
  std::future<redisReplyPtr> ret = execute(buffer, len);
  free(buffer);
  return ret;
}

void QClient::startEventLoop()
{
  connect();
  eventLoopThread = std::thread(&QClient::eventLoop, this);
}

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

    // new request to process
    if (!promises.empty()) {
      redisReply* rr = (redisReply*) reply;

      if (transparentRedirects && rr->type == REDIS_REPLY_ERROR &&
          strncmp(rr->str, "MOVED ", strlen("MOVED ")) == 0) {
        std::vector<std::string> response = split(std::string(rr->str, rr->len), " ");
        RedisServer redirect;

        if (response.size() == 3 && parseServer(response[2], redirect)) {
          redirectedEndpoint = Endpoint(redirect.host, redirect.port);
          return false;
        }
      }

      promises.front().set_value(redisReplyPtr((redisReply*) reply, freeReplyObject));
      promises.pop();
    }
  }

  return true;
}

void QClient::cleanup()
{
  if(networkStream) delete networkStream;
  networkStream = nullptr;

  sock.store(-1);

  if (reader != nullptr) {
    redisReaderFree(reader);
    reader = nullptr;
  }

  // return NULL to all pending requests
  while (!promises.empty()) {
    promises.front().set_value(redisReplyPtr());
    promises.pop();
  }
}

void QClient::connectTCP()
{
  networkStream = new NetworkStream(targetEndpoint.getHost(), targetEndpoint.getPort(), tlsconfig);
  if(!networkStream->ok()) {
    available = false;
    return;
  }

  sock.store(networkStream->getFd());
}

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

  if (!handshakeCommand.empty()) {
    execute(handshakeCommand);
  }
}

void QClient::eventLoop()
{
  const size_t BUFFER_SIZE = 1024 * 2;
  char buffer[BUFFER_SIZE];
  signal(SIGPIPE, SIG_IGN);
  std::chrono::milliseconds backoff(1);

  while (true) {
    std::unique_lock<std::recursive_mutex> lock(mtx);
    struct pollfd polls[2];
    polls[0].fd = shutdownEventFD.getFD();
    polls[0].events = POLLIN;
    polls[1].fd = sock;
    polls[1].events = POLLIN;

    RecvStatus status(true, 0, 0);
    while (sock > 0) {
      lock.unlock();

      // If the previous iteration returned any bytes at all, try to read again
      // without polling. It could be that there's more data cached inside
      // OpenSSL, which poll() will not detect.

      if(status.bytesRead <= 0) {
        int rpoll = poll(polls, 2, -1);
        if(rpoll < 0 && errno != EINTR) {
          // something's wrong, try to reconnect
          break;
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
  redisReplyPtr reply = HandleResponse({"EXISTS", key});

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error exists key: " + key +
                             ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Wrapper function for del async command
//------------------------------------------------------------------------------
AsyncResponseType
QClient::del_async(const std::string& key)
{
  std::vector<std::string> cmd {"DEL", key};
  return std::make_pair(execute(cmd), std::move(cmd));
}

//------------------------------------------------------------------------------
// Wrapper function for del command
//------------------------------------------------------------------------------
long long int
QClient::del(const std::string& key)
{
  redisReplyPtr reply = HandleResponse(del_async(key));

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error del key: " + key +
                             ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Handle response
//------------------------------------------------------------------------------
redisReplyPtr
QClient::HandleResponse(AsyncResponseType resp)
{
  int num_retries = 5;
  std::chrono::milliseconds delay(10);
  redisReplyPtr reply = resp.first.get();

  // Handle transient errors by resubmitting the request
  while (!reply && (num_retries > 0)) {
    auto futur = execute(resp.second);
    reply = futur.get();
    --num_retries;
    std::this_thread::sleep_for(delay);
  }

  if (!reply) {
    throw std::runtime_error("[FATAL] Unable to connect to KV-backend");
  }

  if (reply->type == REDIS_REPLY_ERROR) {
    auto cmd = resp.second;
    std::ostringstream oss;
    std::copy(cmd.begin(), cmd.end(),
              std::ostream_iterator<std::string>(oss, " "));
    throw std::runtime_error("[FATAL] Error REDIS_REPLY_ERROR for command: "
                             + oss.str());
  }

  return reply;
}

//------------------------------------------------------------------------------
// Handle response - convenience function
//------------------------------------------------------------------------------
redisReplyPtr
QClient::HandleResponse(std::vector<std::string> cmd)
{
  auto future = execute(cmd);
  return HandleResponse(std::make_pair(std::move(future),
                                       std::move(cmd)));
}
