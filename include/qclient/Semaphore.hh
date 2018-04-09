// ----------------------------------------------------------------------
// File: Semaphore.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#ifndef QCLIENT_SEMAPHORE_H
#define QCLIENT_SEMAPHORE_H

#include <mutex>
#include <condition_variable>
#include <iostream>

namespace qclient {

class Semaphore {
public:
  //----------------------------------------------------------------------------
  // Constructor taking initial count of the semaphore.
  //----------------------------------------------------------------------------
  Semaphore(int64_t initialCount) : count(initialCount) {}

  //----------------------------------------------------------------------------
  // Increase semaphore count by one.
  //----------------------------------------------------------------------------
  void up() {
    std::lock_guard<std::mutex> lock(mtx);
    count++;
    cv.notify_one();
  }

  //----------------------------------------------------------------------------
  // Try to decrease count by one - blocks if count is already at zero.
  //----------------------------------------------------------------------------
  void down() {
    std::unique_lock<std::mutex> lock(mtx);

    while(count <= 0) {
      cv.wait_for(lock, std::chrono::seconds(1));
    }

    count--;
    return;
  }

  //----------------------------------------------------------------------------
  // Reset count to given value.
  //----------------------------------------------------------------------------
  void reset(size_t newval) {
    std::lock_guard<std::mutex> lock(mtx);
    count = newval;
    cv.notify_all();
  }

  //----------------------------------------------------------------------------
  // Get semaphore's current value.
  //----------------------------------------------------------------------------
  int64_t getValue() const {
    std::lock_guard<std::mutex> lock(mtx);
    return count;
  }

private:
  mutable std::mutex mtx;
  std::condition_variable cv;
  int64_t count;
};

}

#endif
