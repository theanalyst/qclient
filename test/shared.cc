// ----------------------------------------------------------------------
// File: shared.cc
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

#include "qclient/shared/PersistentSharedHash.hh"
#include "qclient/shared/SharedManager.hh"
#include "qclient/shared/TransientSharedHash.hh"
#include <gtest/gtest.h>

using namespace qclient;

// TEST(SharedHash, StandaloneTests) {
//   SharedHash hash(nullptr, "some-key");

//   ASSERT_EQ(hash.getCurrentVersion(), 0u);

//   std::map<std::string, std::string> contents;
//   contents["brubru"] = "123";
//   contents["qwerty"] = "234";
//   contents["123"] = "456";

//   hash.resilver(5u, std::move(contents));
//   ASSERT_EQ(hash.getCurrentVersion(), 5u);

//   std::string tmp;
//   ASSERT_TRUE(hash.get("brubru", tmp));
//   ASSERT_EQ("123", tmp);

//   ASSERT_TRUE(hash.get("qwerty", tmp));
//   ASSERT_EQ("234", tmp);

//   ASSERT_TRUE(hash.get("123", tmp));
//   ASSERT_EQ("456", tmp);

//   // Ensure "qqq" is empty
//   ASSERT_FALSE(hash.get("qqq", tmp));

//   // Insert entry: "qqq" -> "ppp"
//   ASSERT_TRUE(hash.feedRevision(6u, "qqq", "ppp"));
//   ASSERT_EQ(hash.getCurrentVersion(), 6u);

//   // Try inserts with bad revision number
//   ASSERT_FALSE(hash.feedRevision(5u, "pickles", "are awesome"));
//   ASSERT_FALSE(hash.feedRevision(6u, "pickles", "are awesome"));
//   ASSERT_FALSE(hash.feedRevision(4u, "pickles", "are awesome"));
//   ASSERT_FALSE(hash.feedRevision(8u, "pickles", "are awesome"));
//   ASSERT_FALSE(hash.get("pickles", tmp));

//   // Drop a key
//   ASSERT_TRUE(hash.feedRevision(7u, "123", ""));
//   ASSERT_FALSE(hash.get("123", tmp));
//   ASSERT_EQ(hash.getCurrentVersion(), 7u);

//   // Replace a key
//   ASSERT_TRUE(hash.feedRevision(8u, "qqq", "www"));
//   ASSERT_TRUE(hash.get("qqq", tmp));
//   ASSERT_EQ("www", tmp);
//   ASSERT_EQ(hash.getCurrentVersion(), 8u);
// }

TEST(TransientSharedHash, Standalone) {
  SharedManager mg;

  std::unique_ptr<TransientSharedHash> hash1 = mg.makeTransientSharedHash("some-hash");
  std::unique_ptr<TransientSharedHash> hash2 = mg.makeTransientSharedHash("some-other-hash");
  std::unique_ptr<TransientSharedHash> hash3 = mg.makeTransientSharedHash("some-hash");

  hash1->set("a", "b");

  // In simulation mode, there's no network latency and updates should take
  // effect immediately
  std::string out;
  ASSERT_TRUE(hash3->get("a", out));
  ASSERT_EQ(out, "b");

  ASSERT_FALSE(hash2->get("a", out));
}

