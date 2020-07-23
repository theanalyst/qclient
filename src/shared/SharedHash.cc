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

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedHash::SharedHash(SharedManager *sm, const std::string &key)
: mSharedManager(sm), mKey(key) {

  mPersistent.reset(new PersistentSharedHash(sm, key));
  mTransient = sm->makeTransientSharedHash(key);
}

//------------------------------------------------------------------------------
// Set value
//------------------------------------------------------------------------------
void SharedHash::set(const UpdateBatch &batch) {
  std::unique_lock<std::mutex> lock(mMutex);
  for(auto it = batch.localBegin(); it != batch.localEnd(); it++) {
    mLocal[it->first] = it->second;
  }

  lock.unlock();

  mPersistent->set(batch.getPersistent());
  mTransient->set(batch.getTransient());
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

}
