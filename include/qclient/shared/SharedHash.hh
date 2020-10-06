//------------------------------------------------------------------------------
// File: SharedHash.hh
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

#ifndef QCLIENT_SHARED_HASH_HH
#define QCLIENT_SHARED_HASH_HH

#ifdef EOSCITRINE
#include "common/SharedMutexWrapper.hh"
#else
#include <shared_mutex>
#endif

#include <map>

namespace qclient {

class SharedManager;
class UpdateBatch;
class PersistentSharedHash;
class TransientSharedHash;
class SharedHashSubscriber;
class SharedHashSubscription;

//------------------------------------------------------------------------------
//! Convenience class for Transient + Persistent shared hashes mushed together.
//------------------------------------------------------------------------------
class SharedHash {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedHash(SharedManager *sm, const std::string &key);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SharedHash();

  //----------------------------------------------------------------------------
  //! Set value
  //----------------------------------------------------------------------------
  void set(const UpdateBatch &batch);

  //----------------------------------------------------------------------------
  //! Get value
  //----------------------------------------------------------------------------
  bool get(const std::string &field, std::string& value);

  //----------------------------------------------------------------------------
  //! Get current revision ID of the persistent hash
  //----------------------------------------------------------------------------
  uint64_t getPersistentRevision();

  //----------------------------------------------------------------------------
  //! Subscribe for updates to this hash
  //----------------------------------------------------------------------------
  std::unique_ptr<SharedHashSubscription> subscribe();

private:
  std::shared_ptr<SharedHashSubscriber> mHashSubscriber;

  SharedManager* mSharedManager;
  std::string mKey;

  std::mutex mMutex;
  std::map<std::string, std::string> mLocal;
  std::unique_ptr<PersistentSharedHash> mPersistent;
  std::unique_ptr<TransientSharedHash> mTransient;
};

}

#endif
