//------------------------------------------------------------------------------
// File: RocksDBPersistency.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

// Only include this file if you have the rocksdb headers.

#ifndef __QCLIENT_ROCKSDB_PERSISTENCY_H__
#define __QCLIENT_ROCKSDB_PERSISTENCY_H__

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

#include "BackpressuredQueue.hh"

namespace qclient {

inline int64_t binaryStringToInt(const char* buff) {
  int64_t result;
  memcpy(&result, buff, sizeof(result));
  return be64toh(result);
}

inline void intToBinaryString(int64_t num, char* buff) {
  int64_t be = htobe64(num);
  memcpy(buff, &be, sizeof(be));
}

inline std::string intToBinaryString(int64_t num) {
  char buff[sizeof(num)];
  intToBinaryString(num, buff);
  return std::string(buff, sizeof(num));
}

inline void append_int_to_string(int64_t source, std::ostringstream &target) {
  char buff[sizeof(source)];
  memcpy(&buff, &source, sizeof(source));
  target.write(buff, sizeof(source));
}

inline int64_t fetch_int_from_string(const char *pos) {
  int64_t result;
  memcpy(&result, pos, sizeof(result));
  return result;
}

inline std::string serializeVector(const std::vector<std::string> &vec) {
  std::ostringstream ss;

  for(size_t i = 0; i < vec.size(); i++) {
    append_int_to_string(vec[i].size(), ss);
    ss << vec[i];
  }

  return ss.str();
}

inline void deserializeVector(std::vector<std::string> &vec, const std::string &data) {
  vec.clear();

  const char *pos = data.c_str();
  const char *end = data.c_str() + data.size();

  while(pos < end) {
    int64_t len = fetch_int_from_string(pos);
    pos += sizeof(len);

    vec.emplace_back(pos, len);
    pos += len;
  }
}

inline std::string getKey(ItemIndex index) {
  std::stringstream ss;
  ss << "I" << intToBinaryString(index) << std::endl;
  return ss.str();
}

class RocksDBPersistency : public BackgroundFlusherPersistency {
public:
  RocksDBPersistency(const std::string &path) : dbpath(path) {
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.block_size = 16 * 1024;

    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
    if(!status.ok()) {
      std::cerr << "Unable to open rocksdb persistent queue: " << status.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    startIndex = retrieveCounter("START-INDEX");
    endIndex = retrieveCounter("END-INDEX");
  }

  virtual ~RocksDBPersistency() {
    if(db) {
      delete db;
      db = nullptr;
    }
  }

  virtual void record(ItemIndex index, const std::vector<std::string> &cmd) override {
    if(index != endIndex) {
      std::cerr << "Queue corruption, received unexpected index: " << index << " (current endIndex: " << endIndex << ")" << std::endl;
      exit(EXIT_FAILURE);
    }

    std::string serialization = serializeVector(cmd);
    std::string key = getKey(index);

    rocksdb::WriteBatch batch;
    batch.Put(key, serialization);
    batch.Put("END-INDEX", intToBinaryString(index+1));
    commitBatch(batch);

    endIndex = index + 1;
  }

  virtual ItemIndex getStartingIndex() override {
    return startIndex;
  }

  virtual ItemIndex getEndingIndex() override {
    return endIndex;
  }

  virtual bool retrieve(ItemIndex index, std::vector<std::string> &ret) {
    std::string buffer;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), getKey(index), &buffer);

    if(status.IsNotFound()) {
      return false;
    }

    if(!status.ok()) {
      std::cerr << "Queue corruption, error when retrieving key " << getKey(index) << ": " << status.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    deserializeVector(ret, buffer);
    return true;
  }

  virtual void pop() override {
    if(startIndex >= endIndex) {
      std::cerr << "Queue corruption, cannot pop item. startIndex = " << startIndex << ", endIndex = " << endIndex << std::endl;
      exit(EXIT_FAILURE);
    }

    rocksdb::WriteBatch batch;
    batch.SingleDelete(getKey(startIndex));
    batch.Put("START-INDEX", intToBinaryString(startIndex+1));
    commitBatch(batch);

    startIndex++;
  }

private:
  void commitBatch(rocksdb::WriteBatch &batch) {
    rocksdb::Status st = db->Write(rocksdb::WriteOptions(), &batch);
    if(!st.ok()) {
      std::cerr << "Unable to commit write batch to rocksdb queue: " << st.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  ItemIndex retrieveCounter(const std::string &key) {
    std::string buffer;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &buffer);

    if(status.IsNotFound()) {
      return 0;
    }

    if(!status.ok()) {
      std::cerr << "Queue corruption, error when retrieving key " << key << ": " << status.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    if(buffer.size() != sizeof(ItemIndex)) {
      std::cerr << "Queue corruption, unable to parse value of key " << key << std::endl;
      exit(EXIT_FAILURE);
    }

    return binaryStringToInt(buffer.c_str());
  }

  std::atomic<ItemIndex> startIndex {0};
  std::atomic<ItemIndex> endIndex = {0};

  std::string dbpath;
  rocksdb::DB* db = nullptr;
};

}

#endif
