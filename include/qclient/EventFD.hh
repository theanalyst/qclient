//------------------------------------------------------------------------------
// File: EventFD.hh
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

#ifndef __QCLIENT_EVENTFD_H__
#define __QCLIENT_EVENTFD_H__

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <cstdlib>
#include <fcntl.h>

namespace qclient {

// We use pipe for now, not the kernel sys/eventfd.h. :P Not supported on Mac OS.

class EventFD {
public:
  EventFD() {
    int status = pipe(fildes);
    if(status != 0) {
      std::cerr << "EventFD: CRITICAL: Could not obtain file descriptors for EventFD class, errno = " << errno << std::endl;
      std::abort();
    }

    for(size_t i = 0; i < 2; i++) {
      int flags = fcntl(fildes[i], F_GETFL, 0);
      int status = fcntl(fildes[i], F_SETFL, flags | O_NONBLOCK);
      if(status != 0) {
        std::cerr << "EventFD: CRITICAL: Could not set file descriptor as non-blocking" << std::endl;
        std::abort();
      }
    }
  }

  ~EventFD() {
    close();
  }

  void close() {
    ::close(fildes[0]);
    ::close(fildes[1]);
  }

  void notify() {
    char val = 1;
    int rc = write(fildes[1], &val, sizeof(val));

    if (rc != sizeof(val)) {
      std::cerr << "qclient: CRITICAL: could not write to EventFD pipe, return code "
                << rc << ": " << strerror(errno) << std::endl;
    }
  }

  inline int getFD() const {
    return fildes[0];
  }

  void clear() {
    while(true) {
      char buffer[128];
      int rc = ::read(fildes[0], buffer, 64);

      if(rc <= 0) {
        break;
      }
    }
  }

private:
  int fildes[2];
};

}

#endif
