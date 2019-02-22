//------------------------------------------------------------------------------
// File: SharedManager.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef QCLIENT_SHARED_MANAGER_HH
#define QCLIENT_SHARED_MANAGER_HH

#include "../AssistedThread.hh"

namespace qclient {

class SharedHash;

//------------------------------------------------------------------------------
//! SharedManager class to babysit SharedHashes and SharedQueues.
//! Do not destroy this object before all hashes and queues it manages!
//------------------------------------------------------------------------------
class SharedManager {
public:
  //----------------------------------------------------------------------------
  //! Empty constructor - SIMULATION mode. Feed fake data through
  //! feedFakeHashEntry().
  //----------------------------------------------------------------------------
  SharedManager();

  //----------------------------------------------------------------------------
  //! Real mode, connecting to a real QDB instance.
  //----------------------------------------------------------------------------




private:
  std::set<SharedHash*> registeredHashes;
  std::mutex hashMutex;
};

}

#endif