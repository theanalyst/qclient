//------------------------------------------------------------------------------
// File: SharedHash.cc
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

#include "qclient/shared/SharedHash.hh"
#include "qclient/shared/PersistentSharedHash.hh"
#include "qclient/shared/TransientSharedHash.hh"
#include "qclient/shared/UpdateBatch.hh"
#include "qclient/shared/SharedManager.hh"
#include "qclient/shared/SharedHashSubscription.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedHash::SharedHash(SharedManager *sm, const std::string &key)
: mSharedManager(sm), mKey(key) {

  mHashSubscriber.reset(new SharedHashSubscriber());
  mPersistent.reset(new PersistentSharedHash(sm, key, mHashSubscriber));
  mTransient = sm->makeTransientSharedHash(key, mHashSubscriber);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SharedHash::~SharedHash() {

}

//------------------------------------------------------------------------------
// Set value
//------------------------------------------------------------------------------
std::future<redisReplyPtr> SharedHash::set(const UpdateBatch &batch) {
  std::unique_lock<std::mutex> lock(mMutex);
  for(auto it = batch.localBegin(); it != batch.localEnd(); it++) {
    mLocal[it->first] = it->second;
  }

  lock.unlock();

  mTransient->set(batch.getTransient());
  std::future<redisReplyPtr> reply = mPersistent->set(batch.getPersistent());
  return reply;
}

//------------------------------------------------------------------------------
// Get value
//------------------------------------------------------------------------------
bool SharedHash::get(const std::string &field, std::string& value) {
  std::unique_lock<std::mutex> lock(mMutex);
  auto it = mLocal.find(field);
  if(it != mLocal.end()) {
    value = it->second;
    return true;
  }

  lock.unlock();

  if(mTransient->get(field, value)) {
    return true;
  }

  if(mPersistent->get(field, value)) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Get current revision ID of the persistent hash
//------------------------------------------------------------------------------
uint64_t SharedHash::getPersistentRevision() {
  return mPersistent->getCurrentVersion();
}

//------------------------------------------------------------------------------
// Subscribe for updates to this hash
//------------------------------------------------------------------------------
std::unique_ptr<SharedHashSubscription> SharedHash::subscribe() {
  return std::unique_ptr<SharedHashSubscription>(
    new SharedHashSubscription(mHashSubscriber));
}

}
