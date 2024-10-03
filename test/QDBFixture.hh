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
#include "qclient/Members.hh"
#include "qclient/MemoryPersistency.hh"
#include <filesystem>
#include <fstream>

using q_item_t = std::vector<std::string>;


class QDBInstance: public ::testing::Test {

protected:
  void SetUp() override {
    std::string qdb_hostport = getenv("EOS_QUARKDB_HOSTPORT") ?
                               getenv("EOS_QUARKDB_HOSTPORT") :
                               "localhost:7777";
    std::string qdb_passwd = getenv("EOS_QUARKDB_PASSWD") ?
                                                          getenv("EOS_QUARKDB_PASSWD") : "";
    std::string qdb_passwd_file = getenv("EOS_QUARKDB_PASSWD_FILE") ?
                                                                    getenv("EOS_QUARKDB_PASSWD_FILE") : "/etc/eos.keytab";
    members = qclient::Members::fromString(qdb_hostport);
    std::cerr << "Using quarkdb at: " << qdb_hostport << std::endl;
    if (qdb_passwd.empty() && !qdb_passwd_file.empty()) {
      // Read the password from the file
      std::ifstream f(qdb_passwd_file);
      std::stringstream buff;
      buff << f.rdbuf();
      qdb_passwd = buff.str();
    }

    tmp_dir = std::filesystem::temp_directory_path() / "rocksdb_persistency_test";
    std::cerr << "Using rocksdb with tmp_dir: " << tmp_dir << std::endl;
    // cleanup first in case there was a previous instance!
    std::filesystem::remove_all(tmp_dir);
    std::filesystem::create_directory(tmp_dir);
  }

  void TearDown() override {
    std::filesystem::remove_all(tmp_dir);
  }

  qclient::Options getQCOpts() {
    qclient::Options opts;
    opts.transparentRedirects = true;
    opts.retryStrategy = qclient::RetryStrategy::WithTimeout(
        std::chrono::seconds(1));
    if (!qdb_passwd.empty()) {
      opts.handshake.reset(new qclient::HmacAuthHandshake(qdb_passwd));
    }
    return opts;
  }

  qclient::Members members;
  std::filesystem::path tmp_dir;
  qclient::Notifier dummyNotifier;
  std::string qdb_passwd;
};

using QDBFlusherInstance = QDBInstance;
#endif // QCLIENT_ROCKSDBPERSISTENCYFIXTURE_HH