//------------------------------------------------------------------------------
// File: NetworkStream.cc
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

#include <sys/types.h>
#include <sys/socket.h>

#include "NetworkStream.hh"

#include <iostream>
#include <unistd.h>
using namespace qclient;

static RecvStatus recvfn(int socket, char *buffer, int len, int timeout) {
  int ret = ::recv(socket, buffer, len, timeout);
  int err = errno;

  // Case 1: EOF, no more data to read, connection is closed
  if(ret == 0) {
    return RecvStatus(false, 0, 0);
  }

  // Case 2: Non-blocking socket: no data to read, connection still alive
  if(ret == -1 && (err == EWOULDBLOCK || err == EAGAIN)) {
    return RecvStatus(true, err, 0);
  }

  // Case 3: Socket error, connection is closed
  if(ret < 0) {
    return RecvStatus(false, ret, 0);
  }

  // Case 4: We have data
  return RecvStatus(true, 0, ret);
}

static LinkStatus sendfn(int socket, const char *buffer, int len, int timeout) {
  return send(socket, buffer, len, timeout);
}

NetworkStream::NetworkStream(const std::string &hst, int prt, TlsConfig tlsconfig)
: host(hst), port(prt) {

  ConnectionInitiator initiator(hst, prt);

  if(!initiator.ok()) {
    localerrno = initiator.getErrno();
    error = initiator.getError();
    isOk = false;
    return;
  }

  fd = initiator.getFd();
  isOk = (fd >= 0);

  if(tlsconfig.active) {
    using std::placeholders::_1;
    using std::placeholders::_2;
    using std::placeholders::_3;

    RecvFunction recvF = std::bind(recvfn, fd, _1, _2, _3);
    SendFunction sendF = std::bind(sendfn, fd, _1, _2, 0);

    tlsfilter = new TlsFilter(tlsconfig, FilterType::CLIENT, recvF, sendF);
  }
}

void NetworkStream::shutdown() {
  if(fd < 0 || fdShutdown) return;

  int ret = ::shutdown(fd, SHUT_RDWR);
  fdShutdown = true;
  isOk = false;

  if(ret != 0) {
    std::cerr << "qclient: Error during socket shutdown for fd " << fd << " towards " << host << ":" << port << ", retval: " << ret
              << ", errno: " << errno << std::endl;
  }

  // Don't close the socket yet! The fd might be in use (ie inside poll)
  // Only close socket on object destruction.
}

void NetworkStream::close() {
  int ret = ::close(fd);
  if(ret != 0) {
    std::cerr << "qclient: Error during socket close for fd " << fd << ", retval: " << ret
              << ", errno: " << errno << std::endl;
  }
  fd = -1;
}

RecvStatus NetworkStream::recv(char *buffer, int len, int timeout) {
  if(tlsfilter) {
    return tlsfilter->recv(buffer, len, 0);
  }

  return recvfn(fd, buffer, len, 0);
}

LinkStatus NetworkStream::send(const char *buff, int len) {
  if(tlsfilter) {
    return tlsfilter->send(buff, len);
  }

  return ::send(fd, buff, len, 0);
}

NetworkStream::~NetworkStream() {
  if(tlsfilter) delete tlsfilter;
  if(fd > 0) {
    shutdown();
    close();
  }
}
