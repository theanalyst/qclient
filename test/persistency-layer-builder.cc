// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2024 CERN/Switzerland                           *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************
//

#include "gtest/gtest.h"
#include "qclient/PersistencyLayerBuilder.hh"

using namespace qclient;
TEST(PersistencyLayerBuilder, makeFlusherPersistency)
{
  qclient::PersistencyLayerBuilder builder(PersistencyLayerT::MEMORY, FlusherQueueHandler::Serial);
  auto persistency = builder.makeFlusherPersistency();
  ASSERT_TRUE(persistency != nullptr);
}

TEST(PersistencyLayerBuilder, config_memory_multi)
{
  qclient::PersistencyLayerBuilder builder("MEMORY_MULTI");
  ASSERT_EQ(builder.getQueueHandler(), FlusherQueueHandler::LockFree);
  ASSERT_EQ(builder.getPersistencyType(), PersistencyLayerT::MEMORY);
}

TEST(PersistencyLayerBuilder, config_memory)
{
  qclient::PersistencyLayerBuilder builder("MEMORY");
  ASSERT_EQ(builder.getQueueHandler(), FlusherQueueHandler::Serial);
  ASSERT_EQ(builder.getPersistencyType(), PersistencyLayerT::MEMORY);
}

TEST(PersistencyLayerBuilder, config_rocksdb)
{
  qclient::PersistencyLayerBuilder builder("ROCKSDB");
  ASSERT_EQ(builder.getQueueHandler(), FlusherQueueHandler::Serial);
  ASSERT_EQ(builder.getPersistencyType(), PersistencyLayerT::ROCKSDB);
}

TEST(PersistencyLayerBuilder, config_rocksdb_multi)
{
  qclient::PersistencyLayerBuilder builder("ROCKSDB_MULTI");
  ASSERT_EQ(builder.getQueueHandler(), FlusherQueueHandler::LockFree);
  ASSERT_EQ(builder.getPersistencyType(), PersistencyLayerT::ROCKSDB);
}

TEST(PersistencyLayerBuilder, rocksdb_options)
{
  qclient::PersistencyLayerBuilder builder("ROCKSDB",
                                           RocksDBConfig("/tmp", "options"));
  ASSERT_EQ(builder.getQueueHandler(), FlusherQueueHandler::Serial);
  ASSERT_EQ(builder.getPersistencyType(), PersistencyLayerT::ROCKSDB);
}
