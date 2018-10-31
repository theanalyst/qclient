//------------------------------------------------------------------------------
// File: FaultInjector.hh
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

#ifndef QCLIENT_FAULT_INJECTOR_HH
#define QCLIENT_FAULT_INJECTOR_HH

#include "qclient/Members.hh"
#include <set>
#include <mutex>

namespace qclient {

class QClient;
class EventFD;

//------------------------------------------------------------------------------
// Class used to inject faults between client and server cluster. Only network
// partitions supported for now.
//------------------------------------------------------------------------------
class FaultInjector {
public:
  //----------------------------------------------------------------------------
  // Enforce total blackout: This QClient cannot communicate with anyone.
  //----------------------------------------------------------------------------
  void enforceTotalBlackout();

  //----------------------------------------------------------------------------
  // Lift total blackout: Any explicitly added partitions still take effect.
  //----------------------------------------------------------------------------
  void liftTotalBlackout();

  //----------------------------------------------------------------------------
  // Add partitioned Endpoint: Until it's removed, we can no longer talk to
  // this node.
  //----------------------------------------------------------------------------
  void addPartition(const Endpoint &endpoint);

  //----------------------------------------------------------------------------
  // Heal partition: If such endpoint exists in our blacklist, remove it.
  //----------------------------------------------------------------------------
  void healPartition(const Endpoint &endpoint);

  //----------------------------------------------------------------------------
  // Heal all partitions - does not affect total blackout setting, just
  // explicitly added partitions
  //----------------------------------------------------------------------------
  void healAllPartitions();

  //----------------------------------------------------------------------------
  // Check if the fault injector allows connecting to this endpoint.
  //----------------------------------------------------------------------------
  bool hasPartition(const Endpoint &endpoint);

private:
  //----------------------------------------------------------------------------
  // Private constructor: Only QClient can initialize me.
  //----------------------------------------------------------------------------
  friend class QClient;
  FaultInjector(QClient &qcl);
  QClient &qcl;

  //----------------------------------------------------------------------------
  // Map containing active network partitions.
  //----------------------------------------------------------------------------
  mutable std::mutex mtx;
  std::set<Endpoint> partitions;
  bool totalBlackout = false;
};

}

#endif