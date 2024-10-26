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

#ifndef QCLIENT_ROCKSDB_PERSISTENCY_HH
#define QCLIENT_ROCKSDB_PERSISTENCY_HH

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/convenience.h>

#include "qclient/BackgroundFlusher.hh"
#include "utils/AckTracker.hh"

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
    InitializeDB();
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

  virtual bool retrieve(ItemIndex index, std::vector<std::string> &ret) override {
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

protected:
  RocksDBPersistency() = default;

  void InitializeDB() {
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.block_size = 16 * 1024;

    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    options.create_if_missing = true;

    rocksdb::DB *ptr = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, dbpath, &ptr);
    db.reset(ptr);

    if(!status.ok()) {
      std::cerr << "Unable to open rocksdb persistent queue: " << status.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    startIndex = retrieveCounter("START-INDEX");
    endIndex = retrieveCounter("END-INDEX");
  }

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
  std::unique_ptr<rocksdb::DB> db;
};

// Writing a custom merge operator for rocksdb to support atomic addition of int64 values
// Rocksdb already has a UInt64AddOperator, but it looks like we do host order
// versus little endianness, or atleast we use inttoBinaryString and binaryStringToInt
// which may not be same as rocksdb's put
class Int64AddOperator: public rocksdb::AssociativeMergeOperator {
public:
  virtual bool Merge(const rocksdb::Slice& key,
        const rocksdb::Slice* existing_value,
        const rocksdb::Slice& value,
        std::string* new_value,
        rocksdb::Logger* logger) const override {
    int64_t existing = 0;
    if(existing_value != nullptr) {
      existing = binaryStringToInt(existing_value->data());
    }

    int64_t toAdd = binaryStringToInt(value.data());
    int64_t result = existing + toAdd;

    *new_value = intToBinaryString(result);
    return true;
  }

  virtual const char* Name() const override {
    return "Int64AddOperator";
  }
};

class ParallelRocksDBPersistency : public RocksDBPersistency {
public:
  ParallelRocksDBPersistency(const std::string& path, const std::string& options,
                             std::unique_ptr<AckTracker> ackTracker) :
      options_str(options), ackTracker(std::move(ackTracker)), RocksDBPersistency() {
    dbpath = path;
    InitializeDB();
  }

  ParallelRocksDBPersistency(const std::string& path, std::string options="") :
      ParallelRocksDBPersistency(path, options, std::make_unique<HighestAckTracker>()) {}

  ~ParallelRocksDBPersistency() override {
    // Commit the current start & end indices to the databases
    // This is explicitly done to ensure that the serial version of the queue
    // can still read the contents allowing for changes, otherwise
    // MergeOperator changes in journal may not be parsed correctly by the
    // classic serial version of the Queue without explicitly initializing the
    // MergeOperator

    std::cerr << "Destroying ParallelRocksDBPersistency: setting indices: ";
    auto end_index = retrieveCounter("END-INDEX");
    std::cerr << "START-INDEX=" << retrieveCounter("START-INDEX") <<
        " END-INDEX=" << end_index << std::endl;
    rocksdb::WriteBatch batch;
    batch.Put("START-INDEX", intToBinaryString(ackTracker->getStartingIndex()));
    batch.Put("END-INDEX", intToBinaryString(end_index));
    commitBatch(batch);
    auto status  = db->Flush(rocksdb::FlushOptions());
    std::cerr << "Flush Status: " << status.ToString() << std::endl;
  }

  ItemIndex record(const std::vector<std::string> &cmd) override {
    std::string serialization = serializeVector(cmd);
    ItemIndex index = endIndex++;
    std::string key = getKey(index);

    rocksdb::WriteBatch batch;
    batch.Put(key, serialization);
    batch.Merge("END-INDEX", intToBinaryString(1));
    commitBatch(batch);
    return index;
  }

  void popIndex(ItemIndex index) override {
    rocksdb::WriteBatch batch;
    batch.SingleDelete(getKey(index));
    batch.Merge("START-INDEX", intToBinaryString(1));
    commitBatch(batch);
    ackTracker->ackIndex(index);
  }

  ItemIndex getStartingIndex() override {
    return ackTracker->getStartingIndex();
  }
private:
  void InitializeDB()
  {
    rocksdb::Options opts = setupOptions();
    rocksdb::DB *ptr = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(opts, dbpath, &ptr);
    db.reset(ptr);

    if(!status.ok()) {
      std::cerr << "Unable to open rocksdb persistent queue: " << status.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    startIndex = retrieveCounter("START-INDEX");
    endIndex = retrieveCounter("END-INDEX");
    ackTracker->setStartingIndex(startIndex.load());
  }

  rocksdb::Options makeBaseOptions() {
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.block_size = 16 * 1024;

    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    options.create_if_missing = true;
    options.merge_operator.reset(new Int64AddOperator());
    return options;
  }

  rocksdb::Options setupOptions() {
    rocksdb::Options options = makeBaseOptions();
    if (!options_str.empty()) {
      rocksdb::Options new_options; // If the string is invalid this object is invalid...
      rocksdb::ConfigOptions cfg_options;
      cfg_options.ignore_unknown_options = true;
      auto status = rocksdb::GetOptionsFromString(cfg_options, options, options_str, &new_options);
      if (status.ok()) {
        options = std::move(new_options);
        std::string final_db_options_str, final_cf_options_str, final_cf_options_str2;
        auto st = rocksdb::GetStringFromDBOptions(&final_db_options_str, options);
        st = rocksdb::GetStringFromColumnFamilyOptions(&final_cf_options_str, options);
        std::cerr << "QCLIENT: using DB options set from string: " <<  final_db_options_str << std::endl;
        std::cerr << "QCLIENT: using CF options set from string: " << final_cf_options_str << std::endl;
      } else {
        std::cerr << "QCLIENT: skipping invalid string options" << std::endl;
      }
    }
    return options;
  }

  std::string options_str;
  std::unique_ptr<AckTracker> ackTracker;
};

}

#endif
