//------------------------------------------------------------------------------
// File: FileDescriptor.cc
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

#include "qclient/network/FileDescriptor.hh"
#include <unistd.h>

namespace qclient {

//------------------------------------------------------------------------------
// Constructor - this object takes ownership of the specified file descriptor,
// and will call ::close on it on destruction.
//
// FileDescriptor(-1) means it currently owns nothing.
//------------------------------------------------------------------------------
FileDescriptor::FileDescriptor(int fd_) noexcept : fd(fd_) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileDescriptor::~FileDescriptor() {
  reset();
}

//------------------------------------------------------------------------------
// Reset file descriptor - reset(-1) simply means clearing current ownership.
//------------------------------------------------------------------------------
void FileDescriptor::reset(int newfd) {
  if(fd >= 0) {
    ::close(fd);
    fd = -1;
  }

  fd = newfd;
}

//------------------------------------------------------------------------------
// Only allow moving
//------------------------------------------------------------------------------
FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept {
  reset();
  fd = other.fd;
  other.fd = -1;
}

FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) noexcept {
  reset();
  fd = other.fd;
  other.fd = -1;
  return *this;
}

//------------------------------------------------------------------------------
// Get current file descriptor as integer. Never store it, only pass it to
// system calls.. Never call close() on it, either.
//------------------------------------------------------------------------------
int FileDescriptor::get() const {
  return fd;
}

//------------------------------------------------------------------------------
// Release current file descriptor - return its value, and relinquish
// ownership.
//------------------------------------------------------------------------------
int FileDescriptor::release() {
  int retval = fd;
  fd = -1;
  return retval;
}

//------------------------------------------------------------------------------
// Check if the object is currently managing an fd
//------------------------------------------------------------------------------
FileDescriptor::operator bool() const noexcept {
  return fd >= 0;
}


}
