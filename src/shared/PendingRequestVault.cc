//------------------------------------------------------------------------------
// File: PendingRequestVault.cc
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

#include "qclient/shared/PendingRequestVault.hh"
#include "../Uuid.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PendingRequestVault::PendingRequestVault() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
PendingRequestVault::~PendingRequestVault() {}


//------------------------------------------------------------------------------
// Insert pending request
//------------------------------------------------------------------------------
PendingRequestVault::InsertOutcome PendingRequestVault::insert(const std::string &channel, const std::string &contents,
    std::chrono::steady_clock::time_point timepoint) {

  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  InsertOutcome outcome;
  outcome.id = generateUuid();

  auto mapIter = mPendingRequests.insert(std::make_pair(outcome.id, Item(outcome.id))).first;
  mapIter->second.start = timepoint;
  mapIter->second.lastRetry = timepoint;
  mapIter->second.channel = channel;
  mapIter->second.contents = contents;

  outcome.fut = mapIter->second.promise.get_future();
  mNextToRetry.push_back(outcome.id);
  mapIter->second.listIter = mNextToRetry.end();
  --mapIter->second.listIter;

  return outcome;
}

//------------------------------------------------------------------------------
// Satisfy pending request
//------------------------------------------------------------------------------
bool PendingRequestVault::satisfy(const RequestID &id, CommunicatorReply &&reply) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  auto mapIter = mPendingRequests.find(id);
  if(mapIter == mPendingRequests.end()) {
    return false;
  }

  mapIter->second.promise.set_value(std::move(reply));
  mNextToRetry.erase(mapIter->second.listIter);
  mPendingRequests.erase(mapIter);
  return true;
}

//------------------------------------------------------------------------------
// Get current pending requests
//------------------------------------------------------------------------------
bool PendingRequestVault::size() const {
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mPendingRequests.size();
}


}
