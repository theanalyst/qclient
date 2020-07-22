//------------------------------------------------------------------------------
// File: PersistentSharedHash.cc
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

#include "qclient/shared/PersistentSharedHash.hh"
#include "qclient/Logger.hh"
#include "qclient/utils/Macros.hh"
#include "qclient/MultiBuilder.hh"
#include "qclient/shared/SharedManager.hh"
#include "qclient/QClient.hh"
#include "qclient/SSTR.hh"
#include "qclient/pubsub/Subscriber.hh"
#include "qclient/pubsub/Message.hh"
#include "qclient/ResponseBuilder.hh"
#include <sstream>

namespace qclient {

//------------------------------------------------------------------------------
// Constructor - supply a SharedManager object. I'll keep a reference to it
// throughout my lifetime - don't destroy it before me!
//------------------------------------------------------------------------------
PersistentSharedHash::PersistentSharedHash(SharedManager *sm_, const std::string &key_)
: sm(sm_), key(key_), currentVersion(0u) {

  logger = sm->getLogger();
  qcl = sm->getQClient();
  qcl->attachListener(this);
  subscription = sm->getSubscriber()->subscribe(SSTR("__vhash@" << key));

  using namespace std::placeholders;
  subscription->attachCallback(std::bind(&PersistentSharedHash::processIncoming, this, _1));

  triggerResilvering();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
PersistentSharedHash::~PersistentSharedHash() {
  qcl->detachListener(this);
}

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
bool PersistentSharedHash::get(const std::string &field, std::string& value) {
  checkFuture();

  std::shared_lock<std::shared_timed_mutex> lock(contentsMutex);

  auto it = contents.find(field);
  if(it == contents.end()) {
    return false;
  }

  value = it->second;
  return true;
}

//------------------------------------------------------------------------------
// Set contents of the specified field, or batch of values.
// Not guaranteed to succeed in case of network instabilities.
//------------------------------------------------------------------------------
void PersistentSharedHash::set(const std::string &field, const std::string &value) {
  std::map<std::string, std::string> batch;
  batch[field] = value;
  return this->set(batch);
}

void PersistentSharedHash::set(const std::map<std::string, std::string> &batch) {
  qclient::MultiBuilder multi;
  for(auto it = batch.begin(); it != batch.end(); it++) {
    if(it->second.empty()) {
      multi.emplace_back("VHDEL", key, it->first);
    }
    else {
      multi.emplace_back("VHSET", key, it->first, it->second);
    }
  }

  sm->getQClient()->execute(multi.getDeque());
}

//------------------------------------------------------------------------------
// Delete the specified field.
// Not guaranteed to succeed in case of network instabilities.
//------------------------------------------------------------------------------
void PersistentSharedHash::del(const std::string &field) {
  std::map<std::string, std::string> batch;
  batch[field] = "";
  return this->set(batch);
}

//------------------------------------------------------------------------------
// Get current version
//------------------------------------------------------------------------------
uint64_t PersistentSharedHash::getCurrentVersion() {
  checkFuture();

  std::shared_lock<std::shared_timed_mutex> lock(contentsMutex);
  return currentVersion;
}

//------------------------------------------------------------------------------
// Listen for reconnection events
//------------------------------------------------------------------------------
void PersistentSharedHash::notifyConnectionLost(int64_t epoch, int errc, const std::string &msg) {}

void PersistentSharedHash::notifyConnectionEstablished(int64_t epoch) {
  triggerResilvering();
  checkFuture();
}

//------------------------------------------------------------------------------
// Asynchronously trigger resilvering
//------------------------------------------------------------------------------
void PersistentSharedHash::triggerResilvering() {
  std::lock_guard<std::mutex> lock(futureReplyMtx);
  futureReply = qcl->exec("VHGETALL", key);
}

//------------------------------------------------------------------------------
// Check future
//------------------------------------------------------------------------------
void PersistentSharedHash::checkFuture() {
  std::lock_guard<std::mutex> lock(futureReplyMtx);
  if(futureReply.valid() && futureReply.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
    handleResponse(futureReply.get());
  }
}

//------------------------------------------------------------------------------
// Parse serialized version + string map
//------------------------------------------------------------------------------
bool PersistentSharedHash::parseReply(redisReplyPtr &reply, uint64_t &revision, std::map<std::string, std::string> &contents) {
  contents.clear();

  if(reply == nullptr || reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
    return false;
  }

  if(reply->element[0]->type != REDIS_REPLY_INTEGER) {
    return false;
  }

  revision = reply->element[0]->integer;

  redisReply *contentArray = reply->element[1];
  if(!contentArray || contentArray->type != REDIS_REPLY_ARRAY || contentArray->elements % 2 != 0) {
    return false;
  }

  for(size_t i = 0; i < contentArray->elements; i += 2) {
    if(contentArray->element[i]->type != REDIS_REPLY_STRING || contentArray->element[i+1]->type != REDIS_REPLY_STRING) {
      return false;
    }

    std::string key;
    std::string value;

    key = std::string(contentArray->element[i]->str, contentArray->element[i]->len);
    value = std::string(contentArray->element[i+1]->str, contentArray->element[i+1]->len);

    contents[key] = value;
  }

  return true;
}

//------------------------------------------------------------------------------
// Listen for resilvering responses
//------------------------------------------------------------------------------
void PersistentSharedHash::handleResponse(redisReplyPtr &&reply) {
  uint64_t revision;
  std::map<std::string, std::string> contents;

  if(!parseReply(reply, revision, contents)) {
    QCLIENT_LOG(logger, LogLevel::kWarn, "SharedHash could not parse incoming resilvering message: " <<
      qclient::describeRedisReply(reply));
    return;
  }

  //----------------------------------------------------------------------------
  // VHGETALL parsed successfully, apply
  //----------------------------------------------------------------------------
  return resilver(revision, std::move(contents));
}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void PersistentSharedHash::processIncoming(Message &&msg) {
  checkFuture();

  if(msg.getMessageType() != MessageType::kMessage) return;

  redisReplyPtr payload = ResponseBuilder::parseRedisEncodedString(msg.getPayload());
  if(!payload) return;

  uint64_t revision;
  std::map<std::string, std::string> update;

  if(!parseReply(payload, revision, update)) {
    QCLIENT_LOG(logger, LogLevel::kWarn, "SharedHash could not parse incoming revision update: " <<
      qclient::describeRedisReply(payload));
    return;
  }

  //----------------------------------------------------------------------------
  // Payload parsed successfully, apply
  //----------------------------------------------------------------------------
  if(!feedRevision(revision, update)) {
    triggerResilvering();
  }
}

//------------------------------------------------------------------------------
// Feed a single key-value update. Assumes lock is taken.
//------------------------------------------------------------------------------
void PersistentSharedHash::feedSingleKeyValue(const std::string &key, const std::string &value) {
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
bool PersistentSharedHash::feedRevision(uint64_t revision, const std::map<std::string, std::string> &updates) {
  std::unique_lock<std::shared_timed_mutex> lock(contentsMutex);

  if(revision <= currentVersion) {
    // I have a newer version than current revision, nothing to do
    return true;
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

  for(auto it = updates.begin(); it != updates.end(); it++) {
    feedSingleKeyValue(it->first, it->second);
  }

  currentVersion = revision;
  return true;
}

//------------------------------------------------------------------------------
// Same as above, but the given revision updates only a single
// key-value pair
//------------------------------------------------------------------------------
bool PersistentSharedHash::feedRevision(uint64_t revision, const std::string &key, const std::string &value) {
  std::map<std::string, std::string> batch;
  batch[key] = value;
  return feedRevision(revision, batch);
}

//------------------------------------------------------------------------------
// "Resilver" ṫhe hash, flushing all previous contents with new ones.
//------------------------------------------------------------------------------
void PersistentSharedHash::resilver(uint64_t revision, std::map<std::string, std::string> &&newContents) {
  std::unique_lock<std::shared_timed_mutex> lock(contentsMutex);

  QCLIENT_LOG(logger, LogLevel::kWarn, "SharedHash with key " << key <<
    " being resilvered with revision " << revision << " from " << currentVersion);

  currentVersion = revision;
  contents = std::move(newContents);
}

}
