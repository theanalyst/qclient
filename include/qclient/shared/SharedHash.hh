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

#include <qclient/Reply.hh>
#include <map>
#include <set>
#include <vector>
#include <future>

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
  std::future<redisReplyPtr> set(const UpdateBatch &batch);

  //----------------------------------------------------------------------------
  //! Get value
  //----------------------------------------------------------------------------
  bool get(const std::string &field, std::string& value) const;

  //----------------------------------------------------------------------------
  //! Get a list of values, returns a map of kv pairs of found values, expects
  //! empty map as the out param, returns true if all the values have been found
  //!
  //! @param keys vector of string keys
  //! @param out empty map, which will be populated
  //!
  //! @return true if all keys were found, false otherwise or in case of
  //! non empty map
  //----------------------------------------------------------------------------
  bool get(const std::vector<std::string>& keys,
           std::map<std::string, std::string>& out) const;

  //----------------------------------------------------------------------------
  //! Get the set of keys in the current hash
  //!
  //! @return set of keys in the hash, or empty if none
  //----------------------------------------------------------------------------
  std::set<std::string> getKeys() const;

  //----------------------------------------------------------------------------
  //! Get Local value
  //----------------------------------------------------------------------------
  bool getLocal(const std::string &field, std::string& value) const;

  //----------------------------------------------------------------------------
  //! Get a list of local values, returns a map of key, values of found values
  //----------------------------------------------------------------------------
  bool getLocal(const std::vector<std::string>& keys,
                std::map<std::string, std::string>& out) const;

  //----------------------------------------------------------------------------
  //! Get current revision ID of the persistent hash
  //----------------------------------------------------------------------------
  uint64_t getPersistentRevision();

  //----------------------------------------------------------------------------
  //! Subscribe for updates to this hash
  //----------------------------------------------------------------------------
  std::unique_ptr<SharedHashSubscription> subscribe(bool withCurrentContents = false);

private:
  std::shared_ptr<SharedHashSubscriber> mHashSubscriber;

  SharedManager* mSharedManager;
  std::string mKey;

  mutable std::mutex mMutex;
  std::map<std::string, std::string> mLocal;
  std::unique_ptr<PersistentSharedHash> mPersistent;
  std::unique_ptr<TransientSharedHash> mTransient;
};

}

#endif
