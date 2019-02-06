// ----------------------------------------------------------------------
// File: parsing.cc
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

#include "qclient/ResponseParsing.hh"
#include "qclient/ResponseBuilder.hh"
#include <gtest/gtest.h>

using namespace qclient;

TEST(ResponseParsing, StatusParserErr) {
  redisReplyPtr reply = ResponseBuilder::makeStr("test test");

  StatusParser parser(reply);
  ASSERT_FALSE(parser.ok());
  ASSERT_TRUE(parser.value().empty());
  ASSERT_EQ(parser.err(), "Unexpected reply type; was expecting STATUS, received \"test test\"");

  StatusParser parser2(reply.get());
  ASSERT_FALSE(parser2.ok());
  ASSERT_TRUE(parser2.value().empty());
  ASSERT_EQ(parser2.err(), "Unexpected reply type; was expecting STATUS, received \"test test\"");
}

TEST(ResponseParsing, StatusParser) {
  redisReplyPtr reply = ResponseBuilder::makeStatus("some status");

  StatusParser parser(reply);
  ASSERT_TRUE(parser.ok());
  ASSERT_TRUE(parser.err().empty());
  ASSERT_EQ(parser.value(), "some status");
}

