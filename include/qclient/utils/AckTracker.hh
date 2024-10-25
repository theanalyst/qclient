// File: AckTracker.hh
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


#ifndef QCLIENT_ACKTRACKER_HH
#define QCLIENT_ACKTRACKER_HH

#include <atomic>
#include <set>

namespace qclient {

using ItemIndex = int64_t;

struct AckTracker {
  virtual void ackIndex(ItemIndex index) = 0;
  virtual bool isAcked(ItemIndex index) = 0;
  virtual void setStartingIndex(ItemIndex index) = 0;
  virtual ItemIndex getStartingIndex() = 0;
  virtual ItemIndex getHighestAckedIndex() = 0;
  virtual ~AckTracker() = default;
};

class HighestAckTracker : public AckTracker {
public:
  HighestAckTracker() = default;
  ~HighestAckTracker() override = default;

  void
  ackIndex(ItemIndex index) override
  {
    ItemIndex curr_high = nextIndex.load(std::memory_order_acquire);
    nextIndex.store(std::max(index + 1, curr_high), std::memory_order_release);
  }

  bool
  isAcked(ItemIndex index) override
  {
    return index < nextIndex.load(std::memory_order_acquire);
  }

  ItemIndex
  getStartingIndex() override
  {
    return nextIndex.load(std::memory_order_acquire);
  }

  ItemIndex
  getHighestAckedIndex() override
  {
    return nextIndex.load(std::memory_order_acquire);
  }

  void setStartingIndex(ItemIndex index) override
  {
    std::cerr<<"Storing next Index as" << index << std::endl;
    nextIndex.store(index, std::memory_order_release);
  }
private:
  std::atomic<ItemIndex> nextIndex{0};
};

class LowestAckTracker : public AckTracker {
public:
  LowestAckTracker() = default;
  ~LowestAckTracker() override = default;

  void
  ackIndex(ItemIndex index) override
  {
    std::scoped_lock wr_lock(mtx);
    acked.insert(index);
    updateStartingIndex();
  }

  bool
  isAcked(ItemIndex index) override
  {
    std::scoped_lock rd_lock(mtx);
    if (index < startingIndex) {
      return true;
    }
    return acked.find(index) != acked.end();
  }

  ItemIndex
  getStartingIndex() override
  {
    return startingIndex;
  }

  ItemIndex
  getHighestAckedIndex() override
  {
    std::scoped_lock rd_lock(mtx);
    if (acked.empty()) {
      return 0;
    }
    return *acked.rbegin();
  }

  void setStartingIndex(ItemIndex index) override
  {
    std::scoped_lock wr_lock(mtx);
    startingIndex = index;
  }
private:
  void
  updateStartingIndex()
  {
    while (!acked.empty() && *acked.begin() == startingIndex) {
      acked.erase(acked.begin());
      ++startingIndex;
    }
  }
  std::set<ItemIndex> acked;
  std::mutex mtx;
  ItemIndex startingIndex{0};
};

inline std::unique_ptr<AckTracker> makeAckTracker(std::string_view tracker_type) {
  if (tracker_type == "HIGH") {
    return std::make_unique<HighestAckTracker>();
  } else if (tracker_type == "LOW") {
    return std::make_unique<LowestAckTracker>();
  }
  return nullptr;
}

} // namespace qclient

#endif // EOS_ACKTRACKER_HH
