//------------------------------------------------------------------------------
// File: BackpressureApplier.hh
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

#ifndef QCLIENT_BACKPRESSURE_APPLIER_H
#define QCLIENT_BACKPRESSURE_APPLIER_H

#include "qclient/Semaphore.hh"
#include "qclient/Options.hh"

namespace qclient {

class BackpressureApplier {
public:
  BackpressureApplier(BackpressureStrategy st) : strategy(st), semaphore(1) {
    if(strategy.active()) {
      semaphore.reset(strategy.getRequestLimit());
    }
  }

  void reserve() {
    // Reserve a single slot. If not possible, block.
    if(strategy.active()) {
      semaphore.down();
    }
  }

  void release() {
    // Release a single slot.
    if(strategy.active()) {
      semaphore.up();
    }
  }

private:
  BackpressureStrategy strategy;
  Semaphore semaphore;
};

}

#endif
