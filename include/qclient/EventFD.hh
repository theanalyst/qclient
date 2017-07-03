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
#include <sys/eventfd.h>

namespace qclient {

class EventFD {
public:
  EventFD() {
    fd = eventfd(0, EFD_NONBLOCK);
  }

  ~EventFD() {
    close();
  }

  void close() {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }

  void notify(int64_t val = 1) {
    int rc = write(fd, &val, sizeof(val));

    if (rc != sizeof(val)) {
      std::cerr << "qclient: CRITICAL: could not write to eventFD, return code "
                << rc << ": " << strerror(errno) << std::endl;
    }
  }

  inline int getFD() const {
    return fd;
  }

private:
  int fd = -1;
};

}

#endif
