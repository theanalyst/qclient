//------------------------------------------------------------------------------
// File: FileDescriptor.hh
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

#ifndef QCLIENT_FILE_DESCRIPTOR_HH
#define QCLIENT_FILE_DESCRIPTOR_HH

namespace qclient {

//------------------------------------------------------------------------------
// Move-only type which owns a FileDescriptor, RAII wrapper.
//------------------------------------------------------------------------------
class FileDescriptor {
public:
  //----------------------------------------------------------------------------
  // Constructor - this object takes ownership of the specified file descriptor,
  // and will call ::close on it on destruction.
  //----------------------------------------------------------------------------
  constexpr FileDescriptor() noexcept : fd(-1) {}
  explicit FileDescriptor(int fd) noexcept;

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~FileDescriptor();

  //----------------------------------------------------------------------------
  // No copying
  //----------------------------------------------------------------------------
  FileDescriptor(const FileDescriptor& other) = delete;
  FileDescriptor& operator=(const FileDescriptor& other) = delete;

  //----------------------------------------------------------------------------
  // Only allow moving
  //----------------------------------------------------------------------------
  FileDescriptor(FileDescriptor&& other) noexcept;
  FileDescriptor& operator=(FileDescriptor&& other) noexcept;

  //----------------------------------------------------------------------------
  // Reset file descriptor - reset(-1) simply means clearing current ownership.
  //----------------------------------------------------------------------------
  void reset(int newfd = -1);

  //----------------------------------------------------------------------------
  // Get current file descriptor as integer. Never store it, only pass it to
  // system calls.. Never call close() on it, either.
  //----------------------------------------------------------------------------
  int get() const;

  //----------------------------------------------------------------------------
  // Release current file descriptor - return its value, and relinquish
  // ownership.
  //----------------------------------------------------------------------------
  int release();

  //----------------------------------------------------------------------------
  // Check if the object is currently managing an fd
  //----------------------------------------------------------------------------
  explicit operator bool() const noexcept;


private:
  int fd;
};

}

#endif

