//------------------------------------------------------------------------------
// File: LastNMap.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#ifndef QCLIENT_LAST_N_MAP_HH
#define QCLIENT_LAST_N_MAP_HH

#include "RingBuffer.hh"
#include <map>
#include <mutex>

namespace qclient {

//------------------------------------------------------------------------------
// A simple data structure to hold the "last N" elements put into it.
// Thread-safe.
//------------------------------------------------------------------------------
template<typename K, typename V>
class LastNMap {
public:

  //----------------------------------------------------------------------------
  // A simple data structure to hold the "last N" elements put into it.
  // Thread-safe.
  //----------------------------------------------------------------------------
  LastNMap(size_t n) : mRingBuffer(n) {}

  //----------------------------------------------------------------------------
  // Does the given element exist?
  //----------------------------------------------------------------------------
  bool query(const K& key, V& out) const {
    std::unique_lock<std::mutex> lock(mMutex);
    auto it = mContents.find(key);

    if(it == mContents.end()) {
      return false;
    }

    out = it->second.value;
    return true;
  }

  //----------------------------------------------------------------------------
  // Emplace
  //----------------------------------------------------------------------------
  void insert(const K &k, const V &v) {
    std::unique_lock<std::mutex> lock(mMutex);

    if(mRingBuffer.hasRolledOver()) {
      auto it = mContents.find(mRingBuffer.getNextToEvict());
      if(it != mContents.end()) {
        it->second.count--;

        if(it->second.count == 0) {
          mContents.erase(it);
        }
      }
    }

    mRingBuffer.emplace_back(k);
    mContents[k].count++;
    mContents[k].value = v;
  }

private:
  struct InternalItem {
    uint32_t count = 0;
    V value;
  };

  RingBuffer<K> mRingBuffer;
  std::map<K, InternalItem> mContents;
  mutable std::mutex mMutex;
};

}

#endif
