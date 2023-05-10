//------------------------------------------------------------------------------
// File: TransientSharedHash.cc
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

#include "qclient/shared/TransientSharedHash.hh"
#include "qclient/pubsub/Subscriber.hh"
#include "qclient/pubsub/Message.hh"
#include "SharedSerialization.hh"
#include "qclient/Logger.hh"
#include "qclient/shared/SharedManager.hh"
#include "qclient/shared/SharedHashSubscription.hh"

namespace qclient {

//------------------------------------------------------------------------------
//! Private constructor - use SharedManager to instantiate this object.
//------------------------------------------------------------------------------
TransientSharedHash::TransientSharedHash(SharedManager *sm,
  const std::string &chan, std::unique_ptr<qclient::Subscription> sub,
  const std::shared_ptr<SharedHashSubscriber> &hashSub)
: sharedManager(sm), channel(chan), subscription(std::move(sub)), mHashSubscriber(hashSub) {

  using namespace std::placeholders;
  subscription->attachCallback(std::bind(&TransientSharedHash::processIncoming, this, _1));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
TransientSharedHash::~TransientSharedHash() {}

//------------------------------------------------------------------------------
// Process incoming message
//------------------------------------------------------------------------------
void TransientSharedHash::processIncoming(Message &&msg) {
  if(msg.getMessageType() != MessageType::kMessage || msg.getChannel() != channel) {
    // Ignore, message does not concern us
    return;
  }

  std::map<std::string, std::string> incomingBatch;
  if(!parseBatch(msg.getPayload(), incomingBatch)) {
    QCLIENT_LOG(logger, LogLevel::kError, "Could not parse message payload (length "
                << msg.getPayload().size() << ") received in channel "
                << channel << ", ignoring");
    return;
  }

  // Batch received and parsed, apply
  std::unique_lock<std::mutex> lock(contentsMtx);

  for(auto it = incomingBatch.begin(); it != incomingBatch.end(); it++) {
    contents[it->first] = it->second;
  }

  lock.unlock();

  // Notify subscriber
  if(mHashSubscriber) {
    for(auto it = incomingBatch.begin(); it != incomingBatch.end(); it++) {
      SharedHashUpdate hashUpdate;
      hashUpdate.key = it->first;
      hashUpdate.value = it->second;
      mHashSubscriber->feedUpdate(hashUpdate);
    }
  }
}

//------------------------------------------------------------------------------
// Set key to the given value.
//------------------------------------------------------------------------------
void TransientSharedHash::set(const std::string &key, const std::string &value)
{
  std::map<std::string, std::string> batch;
  batch[key] = value;
  set(batch);
}

//------------------------------------------------------------------------------
// Set a batch of key-value pairs.
//------------------------------------------------------------------------------
void TransientSharedHash::set(const std::map<std::string, std::string> &batch)
{
  std::string serializedBatch = serializeBatch(batch);
  sharedManager->publish(channel, serializedBatch);
}

//------------------------------------------------------------------------------
// Get key, if it exists
//------------------------------------------------------------------------------
bool TransientSharedHash::get(const std::string &key, std::string &value) const
{
  std::lock_guard<std::mutex> lock(contentsMtx);

  auto it = contents.find(key);
  if(it == contents.end()) {
    return false;
  }

  value = it->second;
  return true;
}

//------------------------------------------------------------------------------
// Get the set of keys in the current hash
//------------------------------------------------------------------------------
std::set<std::string> TransientSharedHash::getKeys() const
{
  std::set<std::string> keys;
  std::lock_guard<std::mutex> lock(contentsMtx);

  for (const auto& elem: contents) {
    keys.insert(elem.first);
  }

  return keys;
}
}
