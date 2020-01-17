//------------------------------------------------------------------------------
// File: AsyncConnector.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "qclient/network/AsyncConnector.hh"
#include "qclient/network/HostResolver.hh"
#include <iostream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

namespace qclient {

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

//------------------------------------------------------------------------------
// Constructor - initiate connection towards the given ServiceEndpoint. Does
// not block the calling thread until connected - issues an asynchronous
// request the OS, asking to connect.
//------------------------------------------------------------------------------
AsyncConnector::AsyncConnector(const ServiceEndpoint &endpoint) {
  //----------------------------------------------------------------------------
  // Create the socket..
  //----------------------------------------------------------------------------
  fd = FileDescriptor(socket(endpoint.getAiFamily(), endpoint.getAiSocktype(), endpoint.getAiProtocol()));
  if(fd.get() < 0) {
    localerrno = errno;
    error = SSTR("Unable to create a socket: " << strerror(localerrno));
    return;
  }

#ifndef __APPLE__
  //----------------------------------------------------------------------------
  // Set TCP timeout to 30 sec..
  //----------------------------------------------------------------------------
  int timeout = 30 * 1000;
  if(setsockopt(fd.get(), IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout, sizeof(timeout)) != 0) {
    localerrno = errno;
    std::cout << "qclient: could not set TCP_USER_TIMEOUT: " << strerror(localerrno) << std::endl;
  }
#endif

  //----------------------------------------------------------------------------
  // Make non-blocking..
  //----------------------------------------------------------------------------
  int rv = fcntl(fd.get(), F_SETFL, fcntl(fd.get(), F_GETFL) | O_NONBLOCK);
  if(rv != 0) {
    localerrno = errno;
    error = SSTR("Unable to make socket non-blocking: " << strerror(localerrno));
    fd.reset();
    return;
  }

  //----------------------------------------------------------------------------
  // Initiate connect..
  //----------------------------------------------------------------------------
  const std::vector<char>& addr = endpoint.getAddressBytes();
  rv = ::connect(fd.get(), (const sockaddr*) addr.data(), addr.size());

  if(rv < 0 && errno != EINPROGRESS) {
    localerrno = errno;
    fd.reset();
    error = SSTR("Unable to connect to " << endpoint.getOriginalHostname() << ":" << strerror(localerrno));
    return;
  }
  else if(rv == 0) {
    //--------------------------------------------------------------------------
    // ::connect succeeded immediately
    //--------------------------------------------------------------------------
    finished = true;
  }
}

//------------------------------------------------------------------------------
// Is ::connect ready yet?
//------------------------------------------------------------------------------
bool AsyncConnector::isReady() {
  if(finished || localerrno != 0 || fd.get() < 0) {
    return true;
  }

  //----------------------------------------------------------------------------
  // We don't know yet, need to ask kernel..
  // poll() should be instantaneous here.
  //----------------------------------------------------------------------------
  struct pollfd polls[1];
  polls[0].fd = fd.get();
  polls[0].events = POLLOUT;

  int rpoll = poll(polls, 1, 0);
  if(rpoll == 1) {
    finished = true;
  }

  return finished;
}

//------------------------------------------------------------------------------
// Block until file descriptor is ready, OR a POLLIN event occurs in the
// given shutdown fd.
//
// Return true if file descriptor is ready, false if we had to cancel due
// to events in shutdownFd.
//------------------------------------------------------------------------------
bool AsyncConnector::blockUntilReady(int shutdownFd, std::chrono::seconds timeout) {
  if(finished || localerrno != 0 || fd.get() < 0) {
    return true;
  }

  //----------------------------------------------------------------------------
  // Calculate deadline
  //----------------------------------------------------------------------------
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now()
    + timeout;

  //----------------------------------------------------------------------------
  // Sleep until something happens in either file descriptor..
  //----------------------------------------------------------------------------
  struct pollfd polls[2];
  polls[0].fd = shutdownFd;
  polls[0].events = POLLIN;
  polls[1].fd = fd.get();
  polls[1].events = POLLOUT;

  while(true) {
    //--------------------------------------------------------------------------
    // Timed-out?
    //--------------------------------------------------------------------------
    if(deadline < std::chrono::steady_clock::now()) {
      return false;
    }

    int rpoll = poll(polls, 2, 1);
    if(rpoll < 0 && errno != EINTR) {
      //------------------------------------------------------------------------
      // Something is wrong, bail out
      //------------------------------------------------------------------------
      return false;
    }

    if(rpoll < 0) {
      //------------------------------------------------------------------------
      // EINTR
      //------------------------------------------------------------------------
      continue;
    }

    if(polls[1].revents != 0) {
      //------------------------------------------------------------------------
      // An event on our file descriptor.. we could check POLLOUT and POLLERR,
      // but getsockopt seems more robust, and we can get the errno on failure.
      //------------------------------------------------------------------------
      int valopt = 0;
      socklen_t optlen = sizeof(int);
      if(getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, (void*)(&valopt), &optlen) < 0) {
        //----------------------------------------------------------------------
        // Not really supposed to happen..
        //----------------------------------------------------------------------
        localerrno = errno;
        error = SSTR("Unable to run getsockopt() after poll(), errno=" << localerrno << strerror(localerrno));
        finished = true;
        return true;
      }


      if(valopt == EINTR || valopt == EINPROGRESS) {
        //----------------------------------------------------------------------
        // Strange, but ok.. retry.. might never happen.
        //----------------------------------------------------------------------
        continue;
      }

      finished = true;

      if(valopt != 0) {
        localerrno = valopt;
        error = SSTR("Unable to connect (" << localerrno << ")" << ":" << strerror(localerrno));
        return true;
      }

      //------------------------------------------------------------------------
      // Success, connection is active
      //------------------------------------------------------------------------
      return true;
    }

    if(polls[0].revents != 0) {
      //------------------------------------------------------------------------
      // Signalled to break
      //------------------------------------------------------------------------
      return false;
    }
  }
}

//------------------------------------------------------------------------------
// Has there been an error yet? Note that, if ::connect is still pending,
// there might be an error in the future.
//------------------------------------------------------------------------------
bool AsyncConnector::ok() const {
  return (fd.get() > 0) && (localerrno == 0) && (error.empty());
}

//------------------------------------------------------------------------------
// Get file descriptor - could be -1 if an error has occurred.
//------------------------------------------------------------------------------
int AsyncConnector::release() {
  return fd.release();
}

//------------------------------------------------------------------------------
// If an error has occurred, return its errno. Returns 0 if no errors have
// occurred.
//------------------------------------------------------------------------------
int AsyncConnector::getErrno() const {
  return localerrno;
}

//------------------------------------------------------------------------------
// If an error has occurred, return a string description. Returns empty string
// if no errors have occurred.
//------------------------------------------------------------------------------
std::string AsyncConnector::getError() const {
  return error;
}







}
