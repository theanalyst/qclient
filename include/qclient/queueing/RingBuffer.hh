//------------------------------------------------------------------------------
// File: RingBuffer.hh
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

#ifndef QCLIENT_RING_BUFFER_HH
#define QCLIENT_RING_BUFFER_HH

#include <vector>

namespace qclient {

//------------------------------------------------------------------------------
// A simple, fixed-sized ring buffer. Not thread-safe!
//------------------------------------------------------------------------------
template<typename T>
class RingBuffer {
public:

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  RingBuffer(size_t n) {
    mRing.resize(n);
    mNextToEvict = 0;
    mRollover = false;
  }

  //----------------------------------------------------------------------------
  // Emplace back
  //----------------------------------------------------------------------------
  template<typename... Args>
  void emplace_back(Args&&... args) {
    mRing[mNextToEvict] = T(std::forward<Args>(args)...);
    mNextToEvict++;

    if(mNextToEvict >= mRing.size()) {
      mNextToEvict = 0;
      mRollover = true;
    }
  }

  //----------------------------------------------------------------------------
  // Access next-to-evict
  //----------------------------------------------------------------------------
  T& getNextToEvict() {
    return mRing[mNextToEvict];
  }

  //----------------------------------------------------------------------------
  // Has rolled over?
  //----------------------------------------------------------------------------
  bool hasRolledOver() const {
    return mRollover;
  }

private:
  std::vector<T> mRing;
  size_t mNextToEvict;
  bool mRollover;
};

}

#endif
