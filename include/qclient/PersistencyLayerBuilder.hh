// ----------------------------------------------------------------------
// File: PersistencyLayerBuilder.cc
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

#ifndef QCLIENT_PERSISTENCYLAYERBUILDER_HH
#define QCLIENT_PERSISTENCYLAYERBUILDER_HH

#include "qclient/BackgroundFlusher.hh"
#include "qclient/RocksDBPersistency.hh"
#include "qclient/MemoryPersistency.hh"
#include "qclient/Utils.hh"
#include "utils/AckTracker.hh"

namespace qclient {

enum class PersistencyLayerT {
  MEMORY,
  ROCKSDB,
};

constexpr std::tuple<PersistencyLayerT, FlusherQueueHandlerT>
    PersistencyConfigfromString(std::string_view str)
{
  if (str == "MEMORY_MULTI") {
    return {PersistencyLayerT::MEMORY, FlusherQueueHandlerT::LockFree};
  } else if (str == "MEMORY") {
    return {PersistencyLayerT::MEMORY, FlusherQueueHandlerT::Serial};
  } else if (str == "ROCKSDB_MULTI") {
    return {PersistencyLayerT::ROCKSDB, FlusherQueueHandlerT::LockFree};
  }
  return {PersistencyLayerT::ROCKSDB, FlusherQueueHandlerT::Serial};
}

using q_item_t = std::vector<std::string>;

// Ensure a strong type for RocksDB configuration
// as we're dealing with strings everywhere!
struct RocksDBConfig {
  std::string path;
  std::string options;
  RocksDBConfig() = default;
  RocksDBConfig(std::string p, std::string o) : path(p), options(o) {}
};

class PersistencyLayerBuilder {
public:
  PersistencyLayerBuilder(std::string_view configuration, RocksDBConfig _rocksdb_config = {}):
      rocksdb_config(_rocksdb_config) {
    auto config_str_list = split(std::string(configuration), ":");
    std::tie(persistency_type, q_handler_t) = PersistencyConfigfromString(config_str_list[0]);
    if (config_str_list.size() > 1) {
      ack_tracker_type = config_str_list[1];
    }

  }

  PersistencyLayerBuilder(PersistencyLayerT ptype, FlusherQueueHandlerT qtype,
                          RocksDBConfig _rocksdb_config = {}) :
        persistency_type(ptype), q_handler_t(qtype),
        rocksdb_config(_rocksdb_config)  {}

  std::unique_ptr<BackgroundFlusherPersistency> makeFlusherPersistency() {
    if (q_handler_t == FlusherQueueHandlerT::Serial) {
      if (persistency_type == PersistencyLayerT::MEMORY) {
        return std::make_unique<StubInMemoryPersistency<q_item_t>>();
      } else if (persistency_type == PersistencyLayerT::ROCKSDB) {
        return std::make_unique<RocksDBPersistency>(rocksdb_config.path);
      }
    } else if (q_handler_t == FlusherQueueHandlerT::LockFree) {
      std::unique_ptr<AckTracker> ack_tracker {nullptr};
      if (!ack_tracker_type.empty()) {
        ack_tracker = makeAckTracker(ack_tracker_type);
      }
      if (persistency_type == PersistencyLayerT::MEMORY) {
        if (ack_tracker != nullptr) {
          return std::make_unique<StubInMemoryPersistency<q_item_t, true>>(std::move(ack_tracker));
        }
        return std::make_unique<StubInMemoryPersistency<q_item_t, true>>();
      } else if (persistency_type == PersistencyLayerT::ROCKSDB) {
        if (ack_tracker != nullptr) {
          return std::make_unique<ParallelRocksDBPersistency>(rocksdb_config.path,
                                                              rocksdb_config.options,
                                                              std::move(ack_tracker));
        }
        return std::make_unique<ParallelRocksDBPersistency>(rocksdb_config.path,
                                                            rocksdb_config.options);
      }
    }
  }

  qclient::Options getOptions(Options&& opts_) {
    auto opts = std::move(opts_);
    if (q_handler_t == FlusherQueueHandlerT::LockFree) {
      opts.backpressureStrategy = BackpressureStrategy::RateLimitPendingRequests(1ULL<<22);
    }
    return opts;
  }

  FlusherQueueHandlerT
  getQueueHandler() {
    return q_handler_t;
  }

  PersistencyLayerT getPersistencyType() {
    return persistency_type;
  }

  std::string getAckTrackerType() {
    return ack_tracker_type;
  }
private:
  FlusherQueueHandlerT q_handler_t;
  PersistencyLayerT persistency_type;
  RocksDBConfig rocksdb_config;
  std::string ack_tracker_type;
};

struct BackgroundFlusherBuilder {
  static BackgroundFlusher makeFlusher(Members members, Options &&options,
                                       Notifier &notifier,
                                       std::string_view persistency_type,
                                       RocksDBConfig rocksdb_config={}) {
    PersistencyLayerBuilder builder(persistency_type, rocksdb_config);
    return {std::move(members), builder.getOptions(std::move(options)), notifier,
            builder.makeFlusherPersistency(), builder.getQueueHandler()};
  }
};

} // namespace qclient

#endif // QCLIENT_PERSISTENCYLAYERBUILDER_HH
