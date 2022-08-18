//------------------------------------------------------------------------------
// File: Message.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef QCLIENT_MESSAGE_HH
#define QCLIENT_MESSAGE_HH

#include <string>

namespace qclient {

class MessageParser;

enum class MessageType {
  kSubscribe,
  kPatternSubscribe,
  kUnsubscribe,
  kPatternUnsubscribe,
  kMessage,
  kPatternMessage
};

//------------------------------------------------------------------------------
//! This class models a received redis pub/sub message. Depending on the
//! type of message, the following fields are filled out:
//!
//! - kSubscribe, kUnsubscribe
//!       messageType, activeSubscriptions, channel
//! - kPatternSubscribe, kPatternUnsubscribe
//!       messageType, activeSubscriptions, pattern
//! - kMessage,
//!       messageType, channel, contents
//! - kPatternMessage
//!       messageType, channel, contents, pattern
//------------------------------------------------------------------------------
class Message {
public:

  //----------------------------------------------------------------------------
  //! Empty constructor.
  //----------------------------------------------------------------------------
  Message() {}

  MessageType getMessageType() const {
    return messageType;
  }

  bool hasPattern() const {
    return !pattern.empty();
  }

  const std::string& getPattern() const {
    return pattern;
  }

  const std::string& getChannel() const {
    return channel;
  }

  const std::string& getPayload() const {
    return payload;
  }

  int getActiveSubscriptions() const {
    return activeSubscriptions;
  }

  void clear() {
    messageType = MessageType::kSubscribe;
    activeSubscriptions = 0;

    pattern.clear();
    channel.clear();
    payload.clear();
  }

  //----------------------------------------------------------------------------
  //! Check equality of two messages
  //----------------------------------------------------------------------------
  bool operator==(const Message &other) const {
    return messageType           ==   other.messageType           &&
           activeSubscriptions   ==   other.activeSubscriptions   &&
           pattern               ==   other.pattern               &&
           channel               ==   other.channel               &&
           payload               ==   other.payload;
  }

  //----------------------------------------------------------------------------
  //! "Constructor": Make kMessage.
  //----------------------------------------------------------------------------
  static Message createMessage(const std::string &channel, const std::string &payload) {
    Message out;
    out.messageType = MessageType::kMessage;
    out.channel = channel;
    out.payload = payload;
    return out;
  }

private:
  friend class MessageParser;

  MessageType messageType = MessageType::kSubscribe;
  int activeSubscriptions = 0u;

  std::string pattern;
  std::string channel;
  std::string payload;
};

}

#endif
