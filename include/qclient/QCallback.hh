//------------------------------------------------------------------------------
// File: QCallback.hh
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

#ifndef QCLIENT_QCALLBACK_H
#define QCLIENT_QCALLBACK_H

#include <mutex>
#include <future>
#include <queue>
#include <map>
#include <list>
#include <hiredis/hiredis.h>

namespace qclient {

using redisReplyPtr = std::shared_ptr<redisReply>;

class QCallback {
public:
  QCallback() {}
  virtual ~QCallback() {}
  virtual void handleResponse(redisReplyPtr &&reply) = 0;
};

}

#endif
