//------------------------------------------------------------------------------
// File: SharedSerialization.cc
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

#include "SharedSerialization.hh"
#include "qclient/shared/PendingRequestVault.hh"
#include "qclient/utils/Macros.hh"
#include "qclient/Debug.hh"
#include "BinarySerializer.hh"
#include <iostream>

namespace qclient {

//------------------------------------------------------------------------------
//! Utilities for serializing the payload of messages intended for shared
//! data structures.
//------------------------------------------------------------------------------
std::string serializeBatch(const std::map<std::string, std::string> &batch) {
  std::string retval;

  size_t retvalSize = 8; // 8 bytes for array size
  for(auto it = batch.begin(); it != batch.end(); it++) {
    // 8 bytes for key size + actual key
    retvalSize += 8 + it->first.size();

    // 8 bytes for value size + actual value
    retvalSize += 8 + it->second.size();
  }

  BinarySerializer serializer(retval, retvalSize);

  serializer.appendInt64(batch.size() * 2);

  for(auto it = batch.begin(); it != batch.end(); it++) {
    serializer.appendString(it->first);
    serializer.appendString(it->second);
  }

  return retval;
}

bool parseBatch(const std::string &payload, std::map<std::string, std::string> &out) {
  out.clear();

  BinaryDeserializer deserializer(payload);

  int64_t elements = 0;
  if(!deserializer.consumeInt64(elements)) return false;
  if(elements < 0 || elements % 2 != 0) return false;

  std::string key;
  for(int64_t i = 0; i < elements; i++) {
    std::string value;
    if(!deserializer.consumeString(value)) return false;

    // adjust output
    if(i % 2 != 0) {
      out[key] = value;
    }
    else {
      key = std::move(value);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//! Serialize Communicator request
//------------------------------------------------------------------------------
std::string serializeCommunicatorRequest(const std::string &uuid, const std::string &contents) {
  std::string retval;

  // REQ (string) + uuid (string) + contents (string)
  size_t payloadSize = (8+3) + (8 + uuid.size()) + (8 + contents.size());

  BinarySerializer serializer(retval, payloadSize);
  serializer.appendString("REQ");
  serializer.appendString(uuid);
  serializer.appendString(contents);

  qclient_assert(serializer.getRemaining() == 0);
  return retval;
}

//------------------------------------------------------------------------------
//! Parse Communicator request
//------------------------------------------------------------------------------
bool parseCommunicatorRequest(const std::string &payload, std::string &uuid, std::string &contents) {
  BinaryDeserializer deserializer(payload);

  std::string tmp;
  if(!deserializer.consumeString(tmp)) return false;
  if(tmp != "REQ") return false;
  if(!deserializer.consumeString(uuid)) return false;
  if(!deserializer.consumeString(contents)) return false;
  if(deserializer.bytesLeft() != 0) return false;

  return true;
}

//------------------------------------------------------------------------------
//! Serialize Communicator Reply
//------------------------------------------------------------------------------
std::string serializeCommunicatorReply(const std::string &uuid, const CommunicatorReply &reply) {

  std::string retval;
  // RESP (string) + uuid (string) + status (int64) + contents (string)
  size_t payloadSize = (8 + 4) + (8 + uuid.size()) + (8) + (8 + reply.contents.size());

  BinarySerializer serializer(retval, payloadSize);
  serializer.appendString("RESP");
  serializer.appendString(uuid);
  serializer.appendInt64(reply.status);
  serializer.appendString(reply.contents);

  qclient_assert(serializer.getRemaining() == 0);
  return retval;
}

//------------------------------------------------------------------------------
//! Parse Communicator Reply
//------------------------------------------------------------------------------
bool parseCommunicatorReply(const std::string &payload, CommunicatorReply &reply, std::string &uuid) {
  BinaryDeserializer deserializer(payload);

  std::string tmp;
  if(!deserializer.consumeString(tmp)) return false;
  if(tmp != "RESP") return false;
  if(!deserializer.consumeString(uuid)) return false;
  if(!deserializer.consumeInt64(reply.status)) return false;
  if(!deserializer.consumeString(reply.contents)) return false;
  if(deserializer.bytesLeft() != 0) return false;

  return true;
}

}
