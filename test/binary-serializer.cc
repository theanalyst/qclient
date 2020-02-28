// ----------------------------------------------------------------------
// File: binary-serializer.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "shared/BinarySerializer.hh"
#include <gtest/gtest.h>

using namespace qclient;

TEST(BinarySerializer, BasicSanity) {
  std::string target;
  size_t length = 8 + 3 + 8 + 5;

  BinarySerializer serializer(target, length);
  ASSERT_EQ( (size_t) serializer.getRemaining(), length);

  serializer.appendString("abc");
  serializer.appendString("12345");

  ASSERT_EQ(serializer.getRemaining(), 0);

  BinaryDeserializer deserializer(target);

  std::string tmp;
  ASSERT_TRUE(deserializer.consumeString(tmp));
  ASSERT_EQ(tmp, "abc");

  ASSERT_TRUE(deserializer.consumeString(tmp));
  ASSERT_EQ(tmp, "12345");
  ASSERT_EQ(deserializer.bytesLeft(), 0u);
}
