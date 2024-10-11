// ----------------------------------------------------------------------
// File: MemoryPeristency.hh
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


#ifndef QCLIENT_MEMORYPERSISTENCY_HH
#define QCLIENT_MEMORYPERSISTENCY_HH

#include <shared_mutex>
#include <map>

#include "qclient/BackgroundFlusher.hh"

namespace qclient {


/*
 * This is a dummy in-memory persistency layer that can be used for testing!
 * It should not be used in production other than for testing Persistency Layer
 * performance and correctness.
 * The class name is intentional as it is not supposed to really used!
 */
template <typename QueueItem>
class StubInMemoryPersistency final: public PersistencyLayer<QueueItem>
{
public:
  StubInMemoryPersistency() = default;
  ~StubInMemoryPersistency() override = default;

  void record(ItemIndex index, const QueueItem &item) override
  {
    std::unique_lock<std::shared_mutex> wr_lock(mtx);
    data[index] = item;
    ++endingIndex;
  }

  ItemIndex record(const QueueItem &item) override
  {
    ItemIndex index;
    {
      std::unique_lock<std::shared_mutex> wr_lock(mtx);
      index = endingIndex++;
      data[index] = item;
    }

    return index;
  }

  void pop() override
  {
    std::unique_lock<std::shared_mutex> wr_lock(mtx);
    if(!data.empty())
    {
      data.erase(data.begin());
      ++startingIndex;
    }
  }

  void popIndex(ItemIndex index) override
  {
    std::unique_lock<std::shared_mutex> wr_lock(mtx);
    data.erase(index);
  }

  ItemIndex getStartingIndex() override
  {
    return startingIndex;
  }

  ItemIndex getEndingIndex() override
  {
    return endingIndex;
  }

  bool retrieve(ItemIndex index, QueueItem &ret) override
  {
    std::shared_lock<std::shared_mutex> rd_lock(mtx);
    auto it = data.find(index);
    if(it == data.end())
    {
      return false;
    }
    ret = it->second;
    return true;
  }

private:
  std::shared_mutex mtx;
  std::map<ItemIndex, QueueItem> data;
  std::atomic<ItemIndex> startingIndex{0};
  std::atomic<ItemIndex> endingIndex{0};
};


} // qclient

#endif // QCLIENT_MEMORYPERSISTENCY_HH
