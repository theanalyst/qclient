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

//------------------------------------------------------------------------------
//! This class models a received redis pub/sub message. If pattern is empty,
//! then this is a simple message - if not, it's a pmessage matching a
//! requested channel pattern.
//------------------------------------------------------------------------------
class Message {
public:
  Message(const std::string &pattern, const std::string &ch,
  const std::string &cont)
  : matchedPattern(pattern), channel(ch), contents(cont) {}

  Message(std::string &&pattern, std::string &&ch, std::string &&cont)
  : matchedPattern(std::move(pattern)), channel(std::move(ch)),
  contents(std::move(cont)) {}

  bool isFromPattern() const {
    return ! matchedPattern.empty();
  }

  const std::string& getMatchedPattern() const {
    return matchedPattern;
  }

  const std::string& getChannel() const {
    return channel;
  }

  const std::string& getContents() const {
    return contents;
  }

private:
  const std::string matchedPattern;
  const std::string channel;
  const std::string contents;
};

}

#endif