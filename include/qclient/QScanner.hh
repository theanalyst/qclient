//------------------------------------------------------------------------------
// File: QScanner.hh
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

#ifndef __QCLIENT_QSCANNER_H__
#define __QCLIENT_QSCANNER_H__

#include <sstream>

namespace qclient {

class QScanner {
public:
  QScanner(QClient& cl, const std::string &pattern_, size_t count_ = 100)
  : qcl(cl), pattern(pattern_), count(count_), started(false), currentCursor("0") { }

  std::vector<std::string> next() {
    if(started && currentCursor == "0") {
      return {}; // full scan complete, no more elements
    }

    started = true;

    redisReplyPtr reply = qcl.HandleResponse({"SCAN", currentCursor, "MATCH", pattern, "COUNT", std::to_string(count)});

    // Parse the Redis reply - update cursor
    currentCursor = std::string(reply->element[0]->str, static_cast<unsigned int>(reply->element[0]->len));

    // Build return elements into a vector
    std::vector<std::string> ret;

    // Get arrary part of the response
    redisReply* reply_ptr =  reply->element[1];

    for (unsigned long i = 0; i < reply_ptr->elements; ++i) {
      ret.emplace_back(reply_ptr->element[i]->str, static_cast<unsigned int>(reply_ptr->element[i]->len));
    }

    return ret;
  }

private:
  QClient &qcl;
  std::string pattern;
  size_t count;

  bool started;
  std::string currentCursor;
};

}

#endif
