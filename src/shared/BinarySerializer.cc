//------------------------------------------------------------------------------
// File: BinarySerializer.cc
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

#include "BinarySerializer.hh"
#include <string.h>
#include <iostream>

#ifdef __APPLE__

#include <libkern/OSByteOrder.h>

#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

#endif

namespace qclient {

static void intToBinaryString(int64_t num, char* buff) {
  int64_t be = htobe64(num);
  memcpy(buff, &be, sizeof(be));
}

static int64_t binaryStringToInt(const char* buff) {
  int64_t result;
  memcpy(&result, buff, sizeof(result));
  return be64toh(result);
}


//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
BinarySerializer::BinarySerializer(std::string &trg, size_t size)
: target(trg) {

  target.resize(size);
  currentPosition = 0;
}

//------------------------------------------------------------------------------
// Get current position for write
//------------------------------------------------------------------------------
char* BinarySerializer::pos() {
  return ((char*) target.data()) + currentPosition;
}

//------------------------------------------------------------------------------
// Append int64_t
//------------------------------------------------------------------------------
void BinarySerializer::appendInt64(int64_t num) {
  intToBinaryString(num, pos());
  currentPosition += sizeof(int64_t);
}

//------------------------------------------------------------------------------
// Append raw bytes
//------------------------------------------------------------------------------
void BinarySerializer::appendBytes(const char* source, size_t len) {
  memcpy(pos(), source, len);
  currentPosition += len;
}

//------------------------------------------------------------------------------
// Append string, including the length
//------------------------------------------------------------------------------
void BinarySerializer::appendString(const std::string &str) {
  appendInt64(str.size());
  appendBytes(str.data(), str.size());
}

//------------------------------------------------------------------------------
// Get size remaining
//------------------------------------------------------------------------------
int64_t BinarySerializer::getRemaining() const {
  return target.size() - currentPosition;
}
//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
BinaryDeserializer::BinaryDeserializer(const std::string &src)
: source(src), currentPosition(0) {}

//------------------------------------------------------------------------------
//! Fetch int64_t
//------------------------------------------------------------------------------
bool BinaryDeserializer::consumeInt64(int64_t &out) {
  if(!canConsume(8)) {
    return false;
  }

  out = binaryStringToInt(source.data()+currentPosition);
  currentPosition += 8;
  return true;
}

//------------------------------------------------------------------------------
//! Consume that many raw bytes
//------------------------------------------------------------------------------
bool BinaryDeserializer::consumeRawBytes(std::string &str, size_t sz) {
  if(!canConsume(sz)) {
    return false;
  }

  str.resize(sz);
  memcpy(str.data(), source.data()+currentPosition, sz);
  currentPosition += sz;
  return true;
}

//------------------------------------------------------------------------------
//! Fetch string
//------------------------------------------------------------------------------
bool BinaryDeserializer::consumeString(std::string &str) {
  int64_t sz = 0;
  if(!consumeInt64(sz)) {
    return false;
  }

  return consumeRawBytes(str, sz);
}

//------------------------------------------------------------------------------
//! Get number of bytes left
//------------------------------------------------------------------------------
size_t BinaryDeserializer::bytesLeft() const {
  return (source.size() - currentPosition);
}

//------------------------------------------------------------------------------
//! Check if it's possible to consume N bytes
//------------------------------------------------------------------------------
bool BinaryDeserializer::canConsume(size_t b) const {
  return (source.size() - currentPosition) >= b;
}

}

