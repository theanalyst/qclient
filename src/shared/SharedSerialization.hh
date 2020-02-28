//------------------------------------------------------------------------------
// File: SharedSerialization.hh
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

#ifndef QCLIENT_SHARED_SERIALIZATION_HH
#define QCLIENT_SHARED_SERIALIZATION_HH

#include <map>
#include <string>

namespace qclient {

class CommunicatorReply;

//------------------------------------------------------------------------------
//! Utilities for serializing the payload of messages intended for shared
//! data structures.
//------------------------------------------------------------------------------
std::string serializeBatch(const std::map<std::string, std::string> &batch);
bool parseBatch(const std::string &payload, std::map<std::string, std::string> &out);

//------------------------------------------------------------------------------
//! Utilities for serializing the payload of messages intended for Communicator
//! class.
//------------------------------------------------------------------------------
std::string serializeCommunicatorReply(const std::string &uuid, const CommunicatorReply &reply);
bool parseCommunicatorReply(const std::string &payload, CommunicatorReply &reply, std::string &uuid);


}

#endif
