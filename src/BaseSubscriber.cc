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

#include "qclient/BaseSubscriber.hh"
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

//----------------------------------------------------------------------------
// Make QClient options
//----------------------------------------------------------------------------
static Options makeOptions(SubscriptionOptions &&opts) {
  qclient::Options options;
  options.tlsconfig = opts.tlsconfig;
  options.handshake = std::move(opts.handshake);
  options.logger = opts.logger;
  options.ensureConnectionIsPrimed = true;
  options.retryStrategy = RetryStrategy::NoRetries();
  options.backpressureStrategy = BackpressureStrategy::Default();
  return options;
}

//----------------------------------------------------------------------------
// Constructor taking a list of members for the cluster
//----------------------------------------------------------------------------
BaseSubscriber::BaseSubscriber(const Members &memb,
  std::shared_ptr<MessageListener> list, SubscriptionOptions &&opt)
: members(memb), listener(list),  qcl(members, makeOptions(std::move(opt))) {

  // Invalid listener?
  if(!listener) {
    QCLIENT_LOG(options.logger, LogLevel::kFatal, "Attempted to initialize qclient::BaseSubscriber object with nullptr message listener!");
    std::abort();
  }
}

//------------------------------------------------------------------------------
// Notify of a reconnection in the underlying qclient - re-subscribe
//------------------------------------------------------------------------------
void BaseSubscriber::notifyConnectionEstablished(int64_t epoch) {
  std::unique_lock<std::mutex> lock(mtx);


}

//------------------------------------------------------------------------------
// Subscribe to the given channels, in addition to any other subscriptions
// we may currently have.
//------------------------------------------------------------------------------
void BaseSubscriber::subscribe(const std::vector<std::string> &channels) {
  std::unique_lock<std::mutex> lock(mtx);

}

//------------------------------------------------------------------------------
// Subscribe to the given patterns, in addition to any other subscriptions
// we may currently have.
//------------------------------------------------------------------------------
void BaseSubscriber::psubscribe(const std::vector<std::string> &patterns) {
  std::unique_lock<std::mutex> lock(mtx);

}

//------------------------------------------------------------------------------
// Unsubscribe from the given channels. If an empty vector is given, we are
// unsubscribed from all channels. (but not patterns!)
//------------------------------------------------------------------------------
void BaseSubscriber::unsubscribe(const std::vector<std::string> &channels) {
  std::unique_lock<std::mutex> lock(mtx);

}

//------------------------------------------------------------------------------
// Unsubscribe from the given patterns. If an empty vector is given, we are
// unsubscribed from all patterns. (but not channels!)
//------------------------------------------------------------------------------
void BaseSubscriber::punsubscribe(const std::vector<std::string> &patterns) {
  std::unique_lock<std::mutex> lock(mtx);

}

}