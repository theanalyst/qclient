// ----------------------------------------------------------------------
// File: general.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#include "gtest/gtest.h"
#include "qclient/GlobalInterceptor.hh"

TEST(GlobalInterceptor, BasicSanity) {
  qclient::Endpoint e1("example.com", 1234);
  qclient::Endpoint e2("localhost", 999);
  qclient::Endpoint e3("localhost", 998);

  qclient::GlobalInterceptor::addIntercept(e1, e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e1), e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e2), e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e3), e3);

  qclient::GlobalInterceptor::clearIntercepts();
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e1), e1);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e2), e2);
  ASSERT_EQ(qclient::GlobalInterceptor::translate(e3), e3);
}
