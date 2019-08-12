//------------------------------------------------------------------------------
// File: MessageParser.cc
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

#include "MessageParser.hh"
#include "qclient/pubsub/Message.hh"
#include <string.h>

namespace qclient {

//------------------------------------------------------------------------------
// Return true if reply is the given string.
//------------------------------------------------------------------------------
static bool doesMatchString(const redisReply *reply, const std::string &str) {
  if(reply->type != REDIS_REPLY_STRING) {
    return false;
  }

  if( (size_t)reply->len != str.size()) {
    return false;
  }

  if(str.compare(0, str.size(), reply->str) != 0) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Return true if given reply is a string, and extract it.
//------------------------------------------------------------------------------
static bool extractString(const redisReply *reply, std::string &out) {
  if(reply->type != REDIS_REPLY_STRING) {
    return false;
  }

  out = std::string(reply->str, reply->len);
  return true;
}

//------------------------------------------------------------------------------
// Return true if given reply is an integer, and extract it.
//------------------------------------------------------------------------------
static bool extractInteger(const redisReply *reply, int &out) {
  if(reply->type != REDIS_REPLY_INTEGER) {
    return false;
  }

  out = reply->integer;
  return true;
}

//------------------------------------------------------------------------------
// Given a redisReplyPtr from the server, determine if this is a pub-sub
// message, and if so, parse its contents.
//------------------------------------------------------------------------------
bool MessageParser::parse(redisReplyPtr &&reply, Message &out) {
  out.clear();

  if(!reply) {
    //--------------------------------------------------------------------------
    // Invalid parameter
    //--------------------------------------------------------------------------
    return false;
  }

  size_t baseIdx = 0;

  if(reply->type == REDIS_REPLY_ARRAY) {
    //--------------------------------------------------------------------------
    // Array type, base index starts from 0
    //--------------------------------------------------------------------------
    baseIdx = 0;
  }
  else if(reply->type == REDIS_REPLY_PUSH) {
    //--------------------------------------------------------------------------
    // Push type, ensure first element is pubsub
    //--------------------------------------------------------------------------
    if(strncmp(reply->str, "pubsub", reply->len) != 0) {
      return false;
    }

    baseIdx = 1;
  }
  else {
    //--------------------------------------------------------------------------
    // Nope, can't parse
    //--------------------------------------------------------------------------
    return false;
  }


  //----------------------------------------------------------------------------
  // Is this a kMessage?
  //----------------------------------------------------------------------------
  if(doesMatchString(reply->element[baseIdx], "message")) {
    if(reply->elements != baseIdx+3) return false;
    out.messageType = MessageType::kMessage;

    if(!extractString(reply->element[baseIdx+1], out.channel)) return false;
    if(!extractString(reply->element[baseIdx+2], out.payload)) return false;
    return true;
  }

  //----------------------------------------------------------------------------
  // Is this a kPatternMessage?
  //----------------------------------------------------------------------------
  if(doesMatchString(reply->element[baseIdx], "pmessage")) {
    if(reply->elements != baseIdx+4) return false;
    out.messageType = MessageType::kPatternMessage;

    if(!extractString(reply->element[baseIdx+1], out.pattern)) return false;
    if(!extractString(reply->element[baseIdx+2], out.channel)) return false;
    if(!extractString(reply->element[baseIdx+3], out.payload)) return false;
    return true;
  }

  //----------------------------------------------------------------------------
  // Is this a kSubscribe?
  //----------------------------------------------------------------------------
  if(doesMatchString(reply->element[baseIdx], "subscribe")) {
    if(reply->elements != baseIdx+3) return false;
    out.messageType = MessageType::kSubscribe;

    if(!extractString(reply->element[baseIdx+1], out.channel)) return false;
    if(!extractInteger(reply->element[baseIdx+2], out.activeSubscriptions)) return false;
    return true;
  }

  //----------------------------------------------------------------------------
  // Is this a kPatternSubscribe?
  //----------------------------------------------------------------------------
  if(doesMatchString(reply->element[baseIdx], "psubscribe")) {
    if(reply->elements != baseIdx+3) return false;
    out.messageType = MessageType::kPatternSubscribe;

    if(!extractString(reply->element[baseIdx+1], out.pattern)) return false;
    if(!extractInteger(reply->element[baseIdx+2], out.activeSubscriptions)) return false;
    return true;
  }

  //----------------------------------------------------------------------------
  // Is this a kUnsubscribe?
  //----------------------------------------------------------------------------
  if(doesMatchString(reply->element[baseIdx], "unsubscribe")) {
    if(reply->elements != 3) return false;
    out.messageType = MessageType::kUnsubscribe;

    if(!extractString(reply->element[baseIdx+1], out.channel)) return false;
    if(!extractInteger(reply->element[baseIdx+2], out.activeSubscriptions)) return false;
    return true;
  }

  //----------------------------------------------------------------------------
  // Is this a kPatternUnsubscribe?
  //----------------------------------------------------------------------------
  if(doesMatchString(reply->element[baseIdx], "punsubscribe")) {
    if(reply->elements != 3) return false;
    out.messageType = MessageType::kPatternUnsubscribe;

    if(!extractString(reply->element[baseIdx+1], out.pattern)) return false;
    if(!extractInteger(reply->element[baseIdx+2], out.activeSubscriptions)) return false;
    return true;
  }

  return false;
}


}