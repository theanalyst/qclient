//------------------------------------------------------------------------------
// File: NetworkStream.hh
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

#ifndef __QCLIENT_NETWORK_STREAM_H__
#define __QCLIENT_NETWORK_STREAM_H__

#include <string>
#include <atomic>
#include <memory>
#include "qclient/TlsFilter.hh"
#include "qclient/network/HostResolver.hh"

namespace qclient {

class NetworkStream {
public:
  //----------------------------------------------------------------------------
  // Create a network stream based on an existing socket fd.
  //----------------------------------------------------------------------------
  NetworkStream(int fd, TlsConfig tlsconfig);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~NetworkStream();


  bool ok() {
    return isOk;
  }

  int getErrno() {
    return localerrno;
  }

  std::string getError() {
    return error;
  }

  int getFd() {
    return fd;
  }

  void shutdown();
  RecvStatus recv(char *buff, int len, int timeout);
  LinkStatus send(const char *buff, int len);

private:
  //----------------------------------------------------------------------------
  // Initialize TlsFilter
  //----------------------------------------------------------------------------
  void initializeTlsFliter(const TlsConfig &tlsconfig);

  std::string host;
  int port;

  int localerrno = 0;
  std::string error;

  // fd is immutable after construction, safe to access concurrently.
  int fd = -1;

  bool fdShutdown = false;
  std::unique_ptr<TlsFilter> tlsfilter;
  std::atomic<bool> isOk;

  void close();
};


}

#endif
