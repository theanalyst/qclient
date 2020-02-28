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
#include "qclient/utils/Macros.hh"
#include "../Uuid.hh"
#include <iostream>

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

  std::unique_lock<std::mutex> lock(mMutex);

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

  mCV.notify_all();
  qclient_assert(mPendingRequests.size() == mNextToRetry.size());
  return outcome;
}

//------------------------------------------------------------------------------
// Satisfy pending request
//------------------------------------------------------------------------------
bool PendingRequestVault::satisfy(const RequestID &id, CommunicatorReply &&reply) {
  std::unique_lock<std::mutex> lock(mMutex);

  auto mapIter = mPendingRequests.find(id);
  if(mapIter == mPendingRequests.end()) {
    return false;
  }

  mapIter->second.promise.set_value(std::move(reply));
  mNextToRetry.erase(mapIter->second.listIter);
  mPendingRequests.erase(mapIter);
  qclient_assert(mPendingRequests.size() == mNextToRetry.size());
  return true;
}

//------------------------------------------------------------------------------
// Get current pending requests
//------------------------------------------------------------------------------
size_t PendingRequestVault::size() const {
  std::unique_lock<std::mutex> lock(mMutex);
  qclient_assert(mPendingRequests.size() == mNextToRetry.size());
  return mPendingRequests.size();
}

//------------------------------------------------------------------------------
// Get earliest retry
// - Return value False: Vault is empty
// - Return value True: tp is filled with earliest lastRetry time_point
//------------------------------------------------------------------------------
bool PendingRequestVault::getEarliestRetry(std::chrono::steady_clock::time_point &tp) {
  std::unique_lock<std::mutex> lock(mMutex);
  if(mPendingRequests.empty()) {
    return false;
  }

  tp = mPendingRequests[mNextToRetry.front()].lastRetry;
  return true;
}

//------------------------------------------------------------------------------
// Drop front item
//------------------------------------------------------------------------------
void PendingRequestVault::dropFront() {
  mPendingRequests.erase(mNextToRetry.front());
  mNextToRetry.pop_front();
  qclient_assert(mPendingRequests.size() == mNextToRetry.size());
}

//------------------------------------------------------------------------------
// Expire any items which were submitted past the deadline.
// Only the original submission time counts here, not the retries.
//------------------------------------------------------------------------------
size_t PendingRequestVault::expire(std::chrono::steady_clock::time_point deadline) {
  std::unique_lock<std::mutex> lock(mMutex);

  size_t expired = 0;
  while(!mPendingRequests.empty()) {
    if(mPendingRequests[mNextToRetry.front()].start <= deadline) {
      dropFront();
      expired++;
    }
    else {
      break;
    }
  }

  return expired;
}

//------------------------------------------------------------------------------
// Retry front item, if it exists
//------------------------------------------------------------------------------
bool PendingRequestVault::retryFrontItem(std::chrono::steady_clock::time_point now,
  std::string &channel, std::string &contents, std::string &id) {

  std::unique_lock<std::mutex> lock(mMutex);

  if(mPendingRequests.empty()) {
    return false;
  }

  Item& item = mPendingRequests[mNextToRetry.front()];
  channel = item.channel;
  contents = item.contents;
  id = item.id;
  item.lastRetry = now;

  mNextToRetry.pop_front();
  mNextToRetry.emplace_back(item.id);
  qclient_assert(mPendingRequests.size() == mNextToRetry.size());
  return true;
}

//------------------------------------------------------------------------------
// Set blocking mode
//------------------------------------------------------------------------------
void PendingRequestVault::setBlockingMode(bool val) {
  std::unique_lock<std::mutex> lock(mMutex);
  mBlockingMode = val;
  mCV.notify_all();
}

//------------------------------------------------------------------------------
// Block until there's an item in the queue
//------------------------------------------------------------------------------
void PendingRequestVault::blockUntilNonEmpty() {
  std::unique_lock<std::mutex> lock(mMutex);

  while(mBlockingMode && mPendingRequests.empty()) {
    mCV.wait(lock);
  }
}



}
