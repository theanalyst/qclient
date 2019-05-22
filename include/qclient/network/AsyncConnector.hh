//------------------------------------------------------------------------------
// File: AsyncConnector.hh
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

#ifndef QCLIENT_ASYNC_CONNECTOR_HH
#define QCLIENT_ASYNC_CONNECTOR_HH

#include "qclient/Status.hh"
#include "qclient/network/FileDescriptor.hh"

namespace qclient {

class ServiceEndpoint;

//------------------------------------------------------------------------------
// Establishes connection to the specified resolved endpoint asynchronously.
// Does not manage the lifetime of the file descriptor once connected!
//------------------------------------------------------------------------------
class AsyncConnector {
public:
  //----------------------------------------------------------------------------
  // Constructor - initiate connection towards the given ServiceEndpoint. Does
  // not block the calling thread until connected - issues an asynchronous
  // request the OS, asking to connect.
  //----------------------------------------------------------------------------
  AsyncConnector(const ServiceEndpoint &endpoint);

  //----------------------------------------------------------------------------
  // Is ::connect ready yet?
  //----------------------------------------------------------------------------
  bool isReady();

  //----------------------------------------------------------------------------
  // Block until file descriptor is ready, OR a POLLIN event occurs in the
  // given shutdown fd.
  //
  // Return true if file descriptor is ready, false if we had to cancel due
  // to events in shutdownFd.
  //
  // Pass shutdownFd=-1 to disable cancellation, and wait indefinitely until
  // something happens in our file descriptor.
  //----------------------------------------------------------------------------
  bool blockUntilReady(int shutdownFd = -1);

  //----------------------------------------------------------------------------
  // Has there been an error yet? Note that, if ::connect is still pending,
  // there might be an error in the future.
  //----------------------------------------------------------------------------
  bool ok() const;

  //----------------------------------------------------------------------------
  // Get file descriptor - could be -1 if an error has occurred.
  //----------------------------------------------------------------------------
  int release();

  //----------------------------------------------------------------------------
  // If an error has occurred, return its errno. Returns 0 if no errors have
  // occurred.
  //----------------------------------------------------------------------------
  int getErrno() const;

  //----------------------------------------------------------------------------
  // If an error has occurred, return a string description. Returns empty string
  // if no errors have occurred.
  //----------------------------------------------------------------------------
  std::string getError() const;

private:
  FileDescriptor fd;
  int localerrno = 0;
  std::string error;

  bool finished = false;
};

}

#endif
