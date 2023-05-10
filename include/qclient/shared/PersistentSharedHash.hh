//------------------------------------------------------------------------------
// File: SharedHash.hh
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

#ifndef QCLIENT_PERSISTENT_SHARED_HASH_HH
#define QCLIENT_PERSISTENT_SHARED_HASH_HH

#include "qclient/utils/Macros.hh"
#include "qclient/ReconnectionListener.hh"
#include "qclient/Reply.hh"
#include <map>
#include <set>
#include <vector>
#include <string>
#include <future>

#ifdef EOSCITRINE
#include "common/SharedMutexWrapper.hh"
#else
#include <shared_mutex>
#endif

namespace qclient {

//------------------------------------------------------------------------------
//! A "shared hash" class which uses pubsub messages and versioned hashes to
//! synchronize the contents of a hash between multiple clients.
//!
//! It has the following characteristics:
//! - Contents are always persisted.
//! - QuarkDB is the "single source of truth" - local contents will always be
//!   overwritten depending on what is on QDB.
//! - Eventually consistent - if there are network instabilities, it could
//!   get out of sync to the "true" state stored in QDB, but eventually, it
//!   _will_ be consistent.
//! - Not a great idea to store many items in this hash - we regularly need
//!   to fetch the entire contents and overwrite the local map in case of
//!   network instabilities.
//------------------------------------------------------------------------------
class SharedManager; class Logger;
class Subscription; class QClient;
class Message;
class SharedHashSubscriber;
class SharedHashSubscription;

class PersistentSharedHash final : public ReconnectionListener {
public:
  //----------------------------------------------------------------------------
  //! Constructor - supply a SharedManager object. I'll keep a reference to it
  //! throughout my lifetime - don't destroy it before me!
  //----------------------------------------------------------------------------
  PersistentSharedHash(SharedManager *sm, const std::string &key,
    const std::shared_ptr<SharedHashSubscriber> &sub = {} );

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~PersistentSharedHash();

  //----------------------------------------------------------------------------
  //! Read contents of the specified field.
  //!
  //! Eventually consistent read - it could be that a different client has
  //! set this field to a different value _and received an acknowledgement_ at
  //! the time we call get(), but our local value has not been updated yet
  //! due to network latency.
  //!
  //! Returns true if found, false otherwise.
  //----------------------------------------------------------------------------
  bool get(const std::string &field, std::string& value);

  //----------------------------------------------------------------------------
  //! Get the set of keys in the current hash
  //!
  //! @return set of keys in the hash, or empty if none
  //----------------------------------------------------------------------------
  std::set<std::string> getKeys();

  //----------------------------------------------------------------------------
  //! Set contents of the specified field, or batch of values.
  //! Not guaranteed to succeed in case of network instabilities.
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> set(const std::string &field, const std::string &value);
  std::future<redisReplyPtr> set(const std::map<std::string, std::string> &values);

  //----------------------------------------------------------------------------
  //! Delete the specified field.
  //! Not guaranteed to succeed in case of network instabilities.
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> del(const std::string &field);

  //----------------------------------------------------------------------------
  //! Get current version
  //----------------------------------------------------------------------------
  uint64_t getCurrentVersion();

  //----------------------------------------------------------------------------
  //! Listen for reconnection events
  //----------------------------------------------------------------------------
  virtual void notifyConnectionLost(int64_t epoch, int errc, const std::string &msg) override final;
  virtual void notifyConnectionEstablished(int64_t epoch) override final;

  //----------------------------------------------------------------------------
  //! Listen for resilvering responses
  //----------------------------------------------------------------------------
  void handleResponse(redisReplyPtr &&reply);

PUBLIC_FOR_TESTS_ONLY:

  //----------------------------------------------------------------------------
  //! Notify the hash of a new update. Two possibilities:
  //! - The hash is up-to-date, and is able to apply this revision. This
  //!   function returns true.
  //! - The hash is out-of-date, and needs to be reset with the complete
  //!   contents. The change is not applied - a return value of false means
  //!   "please bring me up-to-date by calling resilver function"
  //----------------------------------------------------------------------------
  bool feedRevision(uint64_t revision, const std::map<std::string, std::string>  &updates);

  //----------------------------------------------------------------------------
  //! Same as above, but the given revision updates only a single
  //! key-value pair
  //----------------------------------------------------------------------------
  bool feedRevision(uint64_t revision, const std::string &key, const std::string &value);

  //----------------------------------------------------------------------------
  //! "Resilver" ṫhe hash, flushing all previous contents with new ones.
  //----------------------------------------------------------------------------
  void resilver(uint64_t revision, std::map<std::string, std::string> &&newContents);

private:
  friend class SharedManager;
  friend class SharedHash;

  SharedManager *sm;
  std::string key;
  std::shared_ptr<Logger> logger;

  mutable std::shared_timed_mutex contentsMutex;
  std::map<std::string, std::string> contents;
  uint64_t currentVersion;

  std::unique_ptr<qclient::Subscription> subscription;
  qclient::QClient *qcl = nullptr;

  std::mutex futureReplyMtx;
  std::future<redisReplyPtr> futureReply;

  std::shared_ptr<SharedHashSubscriber> mHashSubscriber;

  //----------------------------------------------------------------------------
  // Feed a single key-value update. Assumes lock is taken.
  //----------------------------------------------------------------------------
  void feedSingleKeyValue(const std::string &key, const std::string &value);

  //----------------------------------------------------------------------------
  // Asynchronously trigger resilvering
  //----------------------------------------------------------------------------
  void triggerResilvering();

  //----------------------------------------------------------------------------
  //! Process incoming message
  //----------------------------------------------------------------------------
  void processIncoming(Message &&msg);

  //----------------------------------------------------------------------------
  //! Check future
  //----------------------------------------------------------------------------
  void checkFuture();

  //----------------------------------------------------------------------------
  //! Parse serialized version + string map
  //----------------------------------------------------------------------------
  bool parseReply(redisReplyPtr &reply, uint64_t &revision, std::map<std::string, std::string> &contents);
};

}

#endif

