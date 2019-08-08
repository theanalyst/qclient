//------------------------------------------------------------------------------
// File: SharedHash.cc
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

#include "qclient/shared/SharedHash.hh"
#include "qclient/Logger.hh"
#include "qclient/utils/Macros.hh"
#include <sstream>

namespace qclient {

//------------------------------------------------------------------------------
// Constructor - supply a SharedManager object. I'll keep a reference to it
// throughout my lifetime - don't destroy it before me!
//------------------------------------------------------------------------------
SharedHash::SharedHash(SharedManager *sm_, const std::string &key_, Logger *lg)
: sm(sm_), key(key_), logger(lg), currentVersion(0u) {}

//------------------------------------------------------------------------------
// Read contents of the specified field.
//
// Eventually consistent read - it could be that a different client has
// set this field to a different value _and received an acknowledgement_ at
// the time we call get(), but our local value has not been updated yet
// due to network latency.
//
// Returns true if found, false otherwise.
//------------------------------------------------------------------------------
bool SharedHash::get(const std::string &field, std::string& value) const {
  std::shared_lock<std::shared_timed_mutex> lock(contentsMutex);

  auto it = contents.find(field);
  if(it == contents.end()) {
    return false;
  }

  value = it->second;
  return true;
}

//------------------------------------------------------------------------------
// Get current version
//------------------------------------------------------------------------------
uint64_t SharedHash::getCurrentVersion() const {
  std::shared_lock<std::shared_timed_mutex> lock(contentsMutex);
  return currentVersion;
}

//------------------------------------------------------------------------------
// Feed a single key-value update. Assumes lock is taken.
//------------------------------------------------------------------------------
void SharedHash::feedSingleKeyValue(const std::string &key, const std::string &value) {
  if(value.empty()) {
    // Deletion
    contents.erase(key);
    return;
  }

  // Insert
  contents[key] = value;
}

//------------------------------------------------------------------------------
// Notify the hash of a new update. Two possibilities:
// - The hash is up-to-date, and is able to apply this revision. This
//   function returns true.
// - The hash is out-of-date, and needs to be reset with the complete
//   contents. The change is not applied - a return value of false means
//   "please bring me up-to-date by calling resilver function"
//------------------------------------------------------------------------------
bool SharedHash::feedRevision(uint64_t revision, const std::vector<std::pair<std::string, std::string>> &updates) {
  std::unique_lock<std::shared_timed_mutex> lock(contentsMutex);

  if(revision <= currentVersion) {
    // not good.. my current version is newer than what QDB has ?!
    // Let's be conservative and ask for a revision, just in case
    QCLIENT_LOG(logger, LogLevel::kError, "SharedHash with key " << key <<
      " appears to have newer revision than server; was fed revision " <<
      revision << ", but current version is " << currentVersion <<
      ", should not happen, asking for resilvering");
    return false;
  }

  if(revision >= currentVersion+2) {
    // We have a discontinuity in received revisions, cannot bring up to date
    // Warn, because this should not happen often, means network instability
    QCLIENT_LOG(logger, LogLevel::kWarn, "SharedHash with key " << key <<
      " went out of date; received revision " << revision << ", but my last " <<
      "version is " << currentVersion << ", asking for resilvering");
    return false;
  }

  qclient_assert(revision == currentVersion+1);

  for(size_t i = 0; i < updates.size(); i++) {
    feedSingleKeyValue(updates[i].first, updates[i].second);
  }

  currentVersion = revision;
  return true;
}

//----------------------------------------------------------------------------
// Same as above, but the given revision updates only a single
// key-value pair
//----------------------------------------------------------------------------
bool SharedHash::feedRevision(uint64_t revision, const std::string &key, const std::string &value) {
  std::vector<std::pair<std::string, std::string>> updates;
  updates.emplace_back(key, value);
  return feedRevision(revision, updates);
}

//------------------------------------------------------------------------------
// "Resilver" ṫhe hash, flushing all previous contents with new ones.
//------------------------------------------------------------------------------
void SharedHash::resilver(uint64_t revision, std::map<std::string, std::string> &&newContents) {
  std::unique_lock<std::shared_timed_mutex> lock(contentsMutex);

  QCLIENT_LOG(logger, LogLevel::kWarn, "SharedHash with key " << key <<
    " being resilvered with revision " << revision << " from " << currentVersion);

  currentVersion = revision;
  contents = std::move(newContents);
}


}
