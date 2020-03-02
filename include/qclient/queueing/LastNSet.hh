//------------------------------------------------------------------------------
// File: LastNSet.hh
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

#ifndef QCLIENT_LAST_N_SET_HH
#define QCLIENT_LAST_N_SET_HH

#include "RingBuffer.hh"
#include <map>
#include <mutex>

namespace qclient {

//------------------------------------------------------------------------------
// A simple data structure to hold the "last N" elements put into it.
// Thread-safe.
//------------------------------------------------------------------------------
template<typename T>
class LastNSet {
public:

  //----------------------------------------------------------------------------
  // A simple data structure to hold the "last N" elements put into it.
  // Thread-safe.
  //----------------------------------------------------------------------------
  LastNSet(size_t n) : mRingBuffer(n) {}

  //----------------------------------------------------------------------------
  // Does the given element exist in the set?
  //----------------------------------------------------------------------------
  bool query(const T& elem) const {
    std::unique_lock<std::mutex> lock(mMutex);
    return mSet.find(elem) != mSet.end();
  }

  //----------------------------------------------------------------------------
  // Emplace
  //----------------------------------------------------------------------------
  template<typename... Args>
  void emplace(Args&&... args) {
    std::unique_lock<std::mutex> lock(mMutex);

    if(mRingBuffer.hasRolledOver()) {
      auto it = mSet.find(mRingBuffer.getNextToEvict());
      if(it != mSet.end()) {
        it->second--;
      }

      if(it->second == 0) {
        mSet.erase(it);
      }
    }

    T item = T(std::forward<Args>(args)...);
    mRingBuffer.emplace_back(item);
    mSet[item]++;
  }

private:
  RingBuffer<T> mRingBuffer;
  std::map<T, uint32_t> mSet;
  mutable std::mutex mMutex;
};

}

#endif
