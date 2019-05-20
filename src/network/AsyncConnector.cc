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
  fd = socket(endpoint.getAiFamily(), endpoint.getAiSocktype(), endpoint.getAiProtocol());
  if(fd == -1) {
    localerrno = errno;
    error = SSTR("Unable to create a socket: " << strerror(localerrno));
    return;
  }

  //----------------------------------------------------------------------------
  // Make non-blocking..
  //----------------------------------------------------------------------------
  int rv = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
  if(rv != 0) {
    localerrno = errno;
    error = SSTR("Unable to make socket non-blocking: " << strerror(localerrno));
    ::close(fd);
    fd = -1;
    return;
  }

  //----------------------------------------------------------------------------
  // Initiate connect..
  //----------------------------------------------------------------------------
  const std::vector<char>& addr = endpoint.getAddressBytes();
  rv = ::connect(fd, (const sockaddr*) addr.data(), addr.size());

  if(rv < 0 && errno != EINPROGRESS) {
    localerrno = errno;
    close(fd);
    fd = -1;
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
  if(finished || localerrno != 0 || fd < 0) {
    return true;
  }

  //----------------------------------------------------------------------------
  // We don't know yet, need to ask kernel..
  // poll() should be instantaneous here.
  //----------------------------------------------------------------------------
  struct pollfd polls[1];
  polls[1].fd = fd;
  polls[1].events = POLLOUT;

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
bool AsyncConnector::blockUntilReady(int shutdownFd) {
  if(finished || localerrno != 0 || fd < 0) {
    return true;
  }

  //----------------------------------------------------------------------------
  // Sleep until something happens in either file descriptor..
  //----------------------------------------------------------------------------
  struct pollfd polls[2];
  polls[0].fd = shutdownFd;
  polls[0].events = POLLIN;
  polls[1].fd = fd;
  polls[1].events = POLLOUT;

  while(true) {
    int rpoll = poll(polls, 2, -1);
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
      // fd seems ready - is it writable though?
      //------------------------------------------------------------------------
      if( (polls[1].revents & POLLOUT) == 0) {
        localerrno = EINVAL;
        error = SSTR("Unable to connect, poll revents: " << polls[0].revents);
      }

      finished = true;
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
  return (fd > 0) && (localerrno == 0) && (error.empty());
}

//------------------------------------------------------------------------------
// Get file descriptor - could be -1 if an error has occurred.
//------------------------------------------------------------------------------
int AsyncConnector::getFd() const {
  return fd;
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
