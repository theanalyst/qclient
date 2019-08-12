// ----------------------------------------------------------------------
// File: response-builder.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#include <gtest/gtest.h>
#include "qclient/ResponseBuilder.hh"
#include "qclient/QClient.hh"

using namespace qclient;

TEST(ResponseBuilder, PushReplies) {
  ResponseBuilder builder;

  builder.feed(">2\r\n$4\r\nabcd\r\n$3\r\naaa\r\n");

  redisReplyPtr reply;
  ASSERT_EQ(builder.pull(reply), ResponseBuilder::Status::kOk);

  ASSERT_EQ(reply->type, REDIS_REPLY_PUSH);
  ASSERT_EQ(describeRedisReply(reply),
    "1) \"abcd\"\n"
    "2) \"aaa\"\n"
  );

}
