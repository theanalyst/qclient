//------------------------------------------------------------------------------
// File: BinarySerializer.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#ifndef QCLIENT_SHARED_BINARY_SERIALIZER_HH
#define QCLIENT_SHARED_BINARY_SERIALIZER_HH

#include <map>
#include <string>

namespace qclient {

//------------------------------------------------------------------------------
//! Helper class for efficiently serializing messages
//------------------------------------------------------------------------------
class BinarySerializer {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  BinarySerializer(std::string &target, size_t size);

  //----------------------------------------------------------------------------
  //! Append int64_t
  //----------------------------------------------------------------------------
  void appendInt64(int64_t num);

  //----------------------------------------------------------------------------
  //! Append raw bytes
  //----------------------------------------------------------------------------
  void appendBytes(const char* source, size_t len);

  //----------------------------------------------------------------------------
  //! Append string, including the length
  //----------------------------------------------------------------------------
  void appendString(const std::string &str);

  //----------------------------------------------------------------------------
  //! Get size remaining
  //----------------------------------------------------------------------------
  int64_t getRemaining() const;

private:
  //----------------------------------------------------------------------------
  //! Get current position for write
  //----------------------------------------------------------------------------
  char* pos();

  std::string &target;
  size_t currentPosition;

};

//------------------------------------------------------------------------------
//! Helper class for efficiently de-serializing messages
//------------------------------------------------------------------------------
class BinaryDeserializer {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  BinaryDeserializer(const std::string &source);

  //----------------------------------------------------------------------------
  //! Consume int64_t
  //----------------------------------------------------------------------------
  bool consumeInt64(int64_t &out);

  //----------------------------------------------------------------------------
  //! Consume string, including its length
  //----------------------------------------------------------------------------
  bool consumeString(std::string &str);

  //----------------------------------------------------------------------------
  //! Consume that many raw bytes
  //----------------------------------------------------------------------------
  bool consumeRawBytes(std::string &str, size_t sz);

  //----------------------------------------------------------------------------
  //! Get number of bytes left
  //----------------------------------------------------------------------------
  size_t bytesLeft() const;


private:
  //----------------------------------------------------------------------------
  //! Check if there's enough space to extract N bytes
  //----------------------------------------------------------------------------
  bool canConsume(size_t b) const;


  const std::string &source;
  size_t currentPosition;

};

}

#endif