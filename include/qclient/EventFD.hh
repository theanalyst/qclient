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

private:
  int fildes[2];
};

}

#endif
