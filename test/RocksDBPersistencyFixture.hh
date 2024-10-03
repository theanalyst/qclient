// ----------------------------------------------------------------------
// File: RocksDBPersistencyFixture.hh
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
  * qclient - A simple redis C++ client with support for redirects       *
  * Copyright (C) 2024 CERN/Switzerland                                  *
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


#ifndef QCLIENT_ROCKSDBPERSISTENCYFIXTURE_HH
#define QCLIENT_ROCKSDBPERSISTENCYFIXTURE_HH

#include "gtest/gtest.h"
#include <filesystem>

// Note that each test using this fixture will have its own instance of rocskdb
// Write a different fixture if you want to reuse!
class RocksDBPersistencyTest : public ::testing::Test {
protected:

  void SetUp() override {
    tmp_dir = std::filesystem::temp_directory_path() / "rocksdb_persistency_test";
    std::cerr << "Testing rocksdb with tmp_dir: " << tmp_dir << std::endl;
    // cleanup first in case there was a previous instance!
    std::filesystem::remove_all(tmp_dir);
    std::filesystem::create_directory(tmp_dir);
  }

  void TearDown() override {
    std::filesystem::remove_all(tmp_dir);
  }

  std::filesystem::path tmp_dir;

};

using RocksDBDeathTest = RocksDBPersistencyTest;

#endif // EOS_ROCKSDBPERSISTENCYFIXTURE_HH
