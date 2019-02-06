//------------------------------------------------------------------------------
// File: ResponseParser.hh
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

#ifndef QCLIENT_RESPONSE_PARSER_HH
#define QCLIENT_RESPONSE_PARSER_HH

#include "qclient/Reply.hh"

namespace qclient {

class StatusParser {
public:

  StatusParser(const redisReply *reply);
  StatusParser(const redisReplyPtr reply);

  bool ok() const;
  std::string err() const;
  std::string value() const;

private:
  bool isOk;
  std::string error;
  std::string val;
};

}

#endif
