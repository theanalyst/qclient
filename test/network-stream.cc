// ----------------------------------------------------------------------
// File: network-stream.cc
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

#include <gtest/gtest.h>
#include "qclient/QClient.hh"
#include <functional>
#include "qclient/network/AsyncConnector.hh"
#include "qclient/network/HostResolver.hh"

using namespace qclient;

TEST(AsyncConnector, noone_is_listening) {
  HostResolver resolver(nullptr);

  Status st;
  std::vector<ServiceEndpoint> endpoints = resolver.resolve("localhost", 13000, st);
  ASSERT_TRUE(st.ok());
  for (const auto& endpoint: endpoints) {
    std::cerr << "Testing endpoint: " << endpoint.getString() << std::endl;
    qclient::AsyncConnector connector(endpoint);
    ASSERT_TRUE(connector.blockUntilReady());
    ASSERT_FALSE(connector.ok());
    ASSERT_EQ(connector.getErrno(), ECONNREFUSED);
  }

}
