// ----------------------------------------------------------------------
// File: BaseSubscriber.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "qclient/pubsub/BaseSubscriber.hh"
#include "qclient/Handshake.hh"
#include "qclient/Logger.hh"
#include "qclient/ReconnectionListener.hh"

namespace qclient {

class BaseSubscriberListener : public qclient::ReconnectionListener {
public:
  BaseSubscriberListener(BaseSubscriber *sub) : subscriber(sub) {}

  virtual void notifyConnectionLost(int64_t epoch, int errc,
    const std::string &msg) override {}

  virtual void notifyConnectionEstablished(int64_t epoch) override {
    subscriber->notifyConnectionEstablished(epoch);
  }

private:
  BaseSubscriber *subscriber = nullptr;
};

//------------------------------------------------------------------------------
// Make QClient options
//------------------------------------------------------------------------------
static Options makeOptions(SubscriptionOptions &&opts,
  std::shared_ptr<MessageListener> listener) {

  qclient::Options options;
  options.tlsconfig = opts.tlsconfig;
  options.handshake = std::move(opts.handshake);
  options.logger = opts.logger;
  options.ensureConnectionIsPrimed = true;
  options.retryStrategy = RetryStrategy::NoRetries();
  options.backpressureStrategy = BackpressureStrategy::Default();
  options.messageListener = listener;

  options.exclusivePubsub = !opts.usePushTypes;
  if(opts.usePushTypes) {
    options.chainHandshake(std::unique_ptr<Handshake>(new ActivatePushTypesHandshake()));
  }

  return options;
}

//------------------------------------------------------------------------------
// Constructor taking a list of members for the cluster
//------------------------------------------------------------------------------
BaseSubscriber::BaseSubscriber(const Members &memb,
  std::shared_ptr<MessageListener> list, SubscriptionOptions &&opt)
: reconnectionListener(new BaseSubscriberListener(this)), members(memb),
  listener(list),
  qcl(members, makeOptions(std::move(opt), list)) {

  // Invalid listener?
  if(!listener) {
    QCLIENT_LOG(options.logger, LogLevel::kFatal, "Attempted to initialize qclient::BaseSubscriber object with nullptr message listener!");
    std::abort();
  }

  qcl.attachListener(reconnectionListener.get());
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BaseSubscriber::~BaseSubscriber() { }

//------------------------------------------------------------------------------
// Notify of a reconnection in the underlying qclient - re-subscribe
// to everything
//------------------------------------------------------------------------------
void BaseSubscriber::notifyConnectionEstablished(int64_t epoch) {
  std::unique_lock<std::mutex> lock(mtx);

  std::vector<std::string> payloadChannels = {"subscribe"};
  for(auto it = channels.begin(); it != channels.end(); it++) {
    payloadChannels.emplace_back(*it);
  }

  std::vector<std::string> payloadPatterns = {"psubscribe"};
  for(auto it = patterns.begin(); it != patterns.end(); it++) {
    payloadPatterns.emplace_back(*it);
  }

  if(payloadChannels.size() != 1) {
    qcl.execute(nullptr, payloadChannels);
  }

  if(payloadPatterns.size() != 1) {
    qcl.execute(nullptr, payloadPatterns);
  }
}

//------------------------------------------------------------------------------
// Subscribe to the given channels, in addition to any other subscriptions
// we may currently have.
//------------------------------------------------------------------------------
void BaseSubscriber::subscribe(const std::vector<std::string> &newChannels) {
  std::unique_lock<std::mutex> lock(mtx);

  std::vector<std::string> payload = {"subscribe"};
  for(auto it = newChannels.begin(); it != newChannels.end(); it++) {
    if(channels.find(*it) == channels.end()) {
      payload.emplace_back(*it);
      channels.emplace(*it);
    }
  }

  if(payload.size() != 1) {
    qcl.execute(nullptr, payload);
  }
}

//------------------------------------------------------------------------------
// Subscribe to the given patterns, in addition to any other subscriptions
// we may currently have.
//------------------------------------------------------------------------------
void BaseSubscriber::psubscribe(const std::vector<std::string> &newPatterns) {
  std::unique_lock<std::mutex> lock(mtx);

  std::vector<std::string> payload = {"psubscribe"};
  for(auto it = newPatterns.begin(); it != newPatterns.end(); it++) {
    if(patterns.find(*it) == patterns.end()) {
      payload.emplace_back(*it);
      patterns.emplace(*it);
    }
  }

  if(payload.size() != 1) {
    qcl.execute(nullptr, payload);
  }
}

//------------------------------------------------------------------------------
// Unsubscribe from the given channels. If an empty vector is given, we are
// unsubscribed from all channels. (but not patterns!)
//------------------------------------------------------------------------------
void BaseSubscriber::unsubscribe(const std::vector<std::string> &remChannels) {
  std::unique_lock<std::mutex> lock(mtx);

  std::vector<std::string> payload = {"unsubscribe"};
  for(auto it = remChannels.begin(); it != remChannels.end(); it++) {
    payload.emplace_back(*it);
    channels.erase(*it);
  }

  if(remChannels.size() == 0) {
    channels.clear();
  }

  qcl.execute(nullptr, payload);
}

//------------------------------------------------------------------------------
// Unsubscribe from the given patterns. If an empty vector is given, we are
// unsubscribed from all patterns. (but not channels!)
//------------------------------------------------------------------------------
void BaseSubscriber::punsubscribe(const std::vector<std::string> &remPatterns) {
  std::unique_lock<std::mutex> lock(mtx);

  std::vector<std::string> payload = {"punsubscribe"};
  for(auto it = remPatterns.begin(); it != remPatterns.end(); it++) {
    payload.emplace_back(*it);
    patterns.erase(*it);
  }

  if(remPatterns.size() == 0) {
    patterns.clear();
  }

  qcl.execute(nullptr, payload);
}

}