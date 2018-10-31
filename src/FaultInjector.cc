// ----------------------------------------------------------------------
// File: FaultInjector.hh
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

#include "qclient/FaultInjector.hh"
#include "qclient/QClient.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FaultInjector::FaultInjector(QClient &q) : qcl(q) {}

//------------------------------------------------------------------------------
// Enforce total blackout
//------------------------------------------------------------------------------
void FaultInjector::enforceTotalBlackout() {
  std::lock_guard<std::mutex> lock(mtx);

  if(!totalBlackout) {
    totalBlackout = true;
    qcl.notifyFaultInjectionsUpdated();
  }
}

//------------------------------------------------------------------------------
// Lift total blackout: Any explicitly added partitions still take effect.
//------------------------------------------------------------------------------
void FaultInjector::liftTotalBlackout() {
  std::lock_guard<std::mutex> lock(mtx);
  totalBlackout = false;
}

//----------------------------------------------------------------------------
// Add partitioned Endpoint: Until it's removed, we can no longer talk to
// this node.
//----------------------------------------------------------------------------
void FaultInjector::addPartition(const Endpoint &endpoint) {
  std::lock_guard<std::mutex> lock(mtx);

  auto it = partitions.emplace(endpoint);
  if(it.second) {
  	qcl.notifyFaultInjectionsUpdated();
  }
}

//------------------------------------------------------------------------------
// Heal partition: If such endpoint exists in our blacklist, remove it.
//------------------------------------------------------------------------------
void FaultInjector::healPartition(const Endpoint &endpoint) {
  std::lock_guard<std::mutex> lock(mtx);
  partitions.erase(endpoint);
}

//------------------------------------------------------------------------------
// Heal all partitions - does not affect total blackout setting, just
// explicitly added partitions
//------------------------------------------------------------------------------
void FaultInjector::healAllPartitions() {
  std::lock_guard<std::mutex> lock(mtx);
  partitions.clear();
}

//----------------------------------------------------------------------------
// Check if the fault injector allows connecting to this endpoint.
//----------------------------------------------------------------------------
bool FaultInjector::hasPartition(const Endpoint &endpoint) {
  std::lock_guard<std::mutex> lock(mtx);
  if(totalBlackout) {
  	return true;
  }

  if(partitions.find(endpoint) != partitions.end()) {
  	return true;
  }

  return false;
}

}
