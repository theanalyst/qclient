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

#include "qclient/qclient.hh"
#include "Utils.hh"

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

using namespace qclient;

//------------------------------------------------------------------------------
// The intercepts machinery
//------------------------------------------------------------------------------

std::map<std::pair<std::string, int>, std::pair<std::string, int>> QClient::intercepts;
std::mutex QClient::interceptsMutex;

void QClient::addIntercept(const std::string &hostname, int port, const std::string &host2, int port2) {
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts[std::make_pair(hostname, port)] = std::make_pair(host2, port2);
}

void QClient::clearIntercepts() {
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts.clear();
}

//------------------------------------------------------------------------------
// QClient class implementation
//------------------------------------------------------------------------------

std::future<redisReplyPtr> QClient::execute(const char *buffer, const size_t len) {
  std::lock_guard<std::recursive_mutex> lock(mtx);

  // not connected?
  if(sock <= 0) {
    std::promise<redisReplyPtr> prom;
    prom.set_value(redisReplyPtr());
    return prom.get_future();
  }

  send(sock, buffer, len, 0);
  promises.emplace();
  return promises.back().get_future();
}

std::future<redisReplyPtr> QClient::execute(size_t nchunks, const char **chunks, const size_t *sizes) {
  char *buffer = NULL;
  int len = redisFormatCommandArgv(&buffer, nchunks, chunks, sizes);
  std::future<redisReplyPtr> ret = this->execute(buffer, len);
  free(buffer);
  return ret;
}

void QClient::startEventLoop() {
  this->connect();
  eventLoopThread = std::thread(&QClient::eventLoop, this);
}


bool QClient::feed(const char *buf, size_t len) {
  if(len > 0) {
    redisReaderFeed(reader, buf, len);
  }

  while(true) {
    void *reply = NULL;
    if(redisReaderGetReply(reader, &reply) == REDIS_ERR) {
      return false;
    }

    if(reply == NULL) break;

    // new request to process
    if(!promises.empty()) {
      redisReply *rr = (redisReply*) reply;
      if(transparentRedirects && rr->type == REDIS_REPLY_ERROR && strncmp(rr->str, "MOVED ", strlen("MOVED ")) == 0) {
        std::vector<std::string> response = split(std::string(rr->str, rr->len), " ");

        RedisServer redirect;
        if(response.size() == 3 && parseServer(response[2], redirect) ) {
          redirectedHost = redirect.host;
          redirectedPort = redirect.port;
          return false;
        }
      }

      promises.front().set_value(redisReplyPtr((redisReply*) reply, freeReplyObject));
      promises.pop();
    }
  }
  return true;
}

void QClient::cleanup() {
  if(sock > 0) {
    std::cerr << "shutting down socket" << std::endl;
    ::shutdown(sock, SHUT_RDWR);
    close(sock);
    sock = -1;
  }

  if(reader != nullptr) {
    redisReaderFree(reader);
    reader = nullptr;
  }

  // return NULL to all pending requests
  while(!promises.empty()) {
    promises.front().set_value(redisReplyPtr());
    promises.pop();
  }
}

void QClient::connectTCP() {
  struct addrinfo hints, *servinfo, *p;

  int tmpsock, rv;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_CANONNAME;

  if((rv = getaddrinfo(targetHost.c_str(), std::to_string(targetPort).c_str(), &hints, &servinfo)) != 0) {
    std::cerr << "qclient: error when resolving " << targetHost << ": " << gai_strerror(rv) << std::endl;
    return;
  }

  // loop through all the results and connect to the first we can
  for(p = servinfo; p != NULL; p = p->ai_next) {
    if((tmpsock = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      continue;
    }

    if(::connect(tmpsock, p->ai_addr, p->ai_addrlen) == -1) {
      close(tmpsock);
      continue;
    }

    break;
  }

  freeaddrinfo(servinfo);

  if(p == NULL) {
    return;
  }

  sock = tmpsock; // atomic set
}

void QClient::connect() {
  std::unique_lock<std::recursive_mutex> lock(mtx);

  cleanup();

  targetHost = host;
  targetPort = port;

  processRedirection();
  discoverIntercept();
  reader = redisReaderCreate();

  connectTCP();

  if(!handshakeCommand.empty()) {
    execute(handshakeCommand);
  }
}

void QClient::eventLoop() {
  const size_t BUFFER_SIZE = 1024 * 2;
  char buffer[BUFFER_SIZE];

  signal(SIGPIPE, SIG_IGN);

  std::chrono::milliseconds backoff(1);
  while(true) {
    std::unique_lock<std::recursive_mutex> lock(mtx);

    struct pollfd polls[2];
    polls[0].fd = shutdownEventFD.getFD();
    polls[0].events = POLLIN;

    polls[1].fd = sock;
    polls[1].events = POLLIN;

    while(sock > 0) {
      lock.unlock();
      poll(polls, 2, 1);
      lock.lock();

      if(shutdown) break;

      // legit connection, reset backoff
      backoff = std::chrono::milliseconds(1);

      if(polls[1].revents != 0) {
        int bytes = recv(sock, buffer, BUFFER_SIZE, 0);

        if(bytes <= 0) {
          break;
        }

        if(!feed(buffer, bytes)) break;
      }
    }

    if(shutdown) {
      feed(NULL, 0);
      break;
    }

    lock.unlock();
    std::this_thread::sleep_for(backoff);
    if(backoff < std::chrono::milliseconds(2048)) {
      backoff++;
    }
    this->connect();
  }
}

QClient::QClient(const std::string &host_, const int port_, bool redirects, std::vector<std::string> handshake)
: host(host_), port(port_), transparentRedirects(redirects), handshakeCommand(handshake) {
  startEventLoop();
}

void QClient::processRedirection() {
  if(!redirectedHost.empty() && redirectedPort > 0) {
    std::cerr << "qclient: redirecting to " << redirectedHost << ":" << redirectedPort << std::endl;
    targetHost = redirectedHost;
    targetPort = redirectedPort;
    redirectionActive = true;
  }
  else if(redirectionActive) {
    std::cerr << "qclient: redirecting back to original host " << host << ":" << port << std::endl;
    redirectionActive = false;
  }

  redirectedHost.clear();
  redirectedPort = -1;
}

void QClient::discoverIntercept() {
  //----------------------------------------------------------------------------
  // If this (host, port) pair is being intercepted, redirect to a different
  // (host, port) pair instead.
  //----------------------------------------------------------------------------
  std::lock_guard<std::mutex> lock(interceptsMutex);

  auto it = intercepts.find(std::make_pair(targetHost, targetPort));
  if(it != intercepts.end()) {
    targetHost = it->second.first;
    targetPort = it->second.second;
  }
}

QClient::~QClient() {
  shutdown = true;
  shutdownEventFD.notify();
  eventLoopThread.join();
  cleanup();
}

std::future<redisReplyPtr> QClient::execute(const std::vector<std::string> &req) {
  const char *cstr[req.size()];
  size_t sizes[req.size()];

  for(size_t i = 0; i < req.size(); i++) {
    cstr[i] = req[i].c_str();
    sizes[i] = req[i].size();
  }

  return execute(req.size(), cstr, sizes);
}
