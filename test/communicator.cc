// ----------------------------------------------------------------------
// File: communicator.cc
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

#include "qclient/shared/PendingRequestVault.hh"
#include <gtest/gtest.h>

using namespace qclient;

TEST(PendingRequestVault, BasicSanity) {
  PendingRequestVault requestVault;
  ASSERT_EQ(requestVault.size(), 0u);

  std::chrono::steady_clock::time_point tp;
  tp += std::chrono::seconds(1);

  PendingRequestVault::InsertOutcome outcome = requestVault.insert("ch1", "123", tp);
  std::cerr << "RequestID: " << outcome.id << std::endl;

  ASSERT_EQ(requestVault.size(), 1u);
  ASSERT_EQ(outcome.fut.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  CommunicatorReply reply;
  reply.status = 123;
  reply.contents = "aaa";

  ASSERT_FALSE(requestVault.satisfy("123", std::move(reply)));
  ASSERT_TRUE(requestVault.satisfy(outcome.id, std::move(reply)));
  ASSERT_EQ(requestVault.size(), 0u);

  CommunicatorReply rep = outcome.fut.get();
  ASSERT_EQ(rep.status, 123);
  ASSERT_EQ(rep.contents, "aaa");
}

