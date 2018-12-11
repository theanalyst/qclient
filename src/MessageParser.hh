//------------------------------------------------------------------------------
// File: MessageParser.hh
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

#ifndef QCLIENT_MESSAGE_PARSER_HH
#define QCLIENT_MESSAGE_PARSER_HH

#include "qclient/Reply.hh"

namespace qclient {

class Message;

class MessageParser {
public:

  //----------------------------------------------------------------------------
  // Given a redisReplyPtr from the server, determine if this is a pub-sub
  // message, and if so, parse its contents.
  //----------------------------------------------------------------------------
  static bool parse(redisReplyPtr &&reply, Message &out);
};

}

#endif
