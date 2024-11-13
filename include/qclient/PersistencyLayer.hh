// ----------------------------------------------------------------------
// File: PeristencyLayer.hh
// Author: Georgios Bitzes, Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                           *
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


#ifndef QCLIENT_PERSISTENCYLAYER_HH
#define QCLIENT_PERSISTENCYLAYER_HH

#include <limits>
#include <cstdint>
#include <vector>
#include <string>

namespace qclient {

//------------------------------------------------------------------------------
// PersistencyLayer interface - inherit from here to implement extra
// functionality. Default implementation does nothing at all.
//------------------------------------------------------------------------------
using ItemIndex = int64_t;

template<typename QueueItem>
class PersistencyLayer {
public:
  PersistencyLayer() {}
  virtual ~PersistencyLayer() {} // very important to be virtual!

  virtual void record(ItemIndex index, const QueueItem &item) {}
  virtual ItemIndex record(const QueueItem &item) { return std::numeric_limits<ItemIndex>::min(); }
  virtual void pop() {}
  virtual void popIndex(ItemIndex index) {}

  // The following three functions are only used during reconstruction.
  virtual ItemIndex getStartingIndex() {
    return 0;
  }

  virtual ItemIndex getEndingIndex() {
    return 0;
  }

  virtual bool retrieve(ItemIndex index, QueueItem &ret) {
    return false;
  }
};

using BackgroundFlusherPersistency = PersistencyLayer<std::vector<std::string>>;

} // qclient

#endif // EOS_PERSISTENCYLAYER_HH
