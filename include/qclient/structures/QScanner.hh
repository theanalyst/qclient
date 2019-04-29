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
#include "qclient/Utils.hh"
#include "qclient/QClient.hh"

namespace qclient {

class QScanner {
public:
  QScanner(QClient& cl, const std::string &pattern_, size_t count_ = 100)
  : qcl(cl), pattern(pattern_), count(count_), cursor("0"), reachedEnd(false) {
    fillFromBackend();
  }

  bool valid() const {
    return ! results.empty();
  }

  void fillFromBackend() {
    while(!reachedEnd && results.empty()) {
      reqs++;

      redisReplyPtr reply = qcl.exec("SCAN", cursor, "MATCH", pattern,
                                     "COUNT", std::to_string(count)).get();

      if (reply == nullptr) {
        throw std::runtime_error("[FATAL] Error scan pattern: " + pattern +
                                 ": Unexpected/null reply");
      }

      // Parse the Redis reply - update cursor
      cursor = std::string(reply->element[0]->str, static_cast<unsigned int>(reply->element[0]->len));

      // Get arrary part of the response
      redisReply* reply_ptr =  reply->element[1];

      for (unsigned long i = 0; i < reply_ptr->elements; ++i) {
        results.emplace_back(reply_ptr->element[i]->str, static_cast<unsigned int>(reply_ptr->element[i]->len));
      }

      if(cursor == "0") {
        reachedEnd = true;
      }
    }
  }

  void next() {
    if(!results.empty()) {
      results.pop_front();
    }
    fillFromBackend();
  }

  const std::string& getValue() const {
    return results.front();
  }

  size_t requestsSoFar() const {
    return reqs;
  }

private:
  qclient::QClient &qcl;
  std::string pattern;
  uint32_t count;
  std::string cursor;
  bool reachedEnd;
  std::deque<std::string> results;
  size_t reqs = 0;
};

}

#endif
