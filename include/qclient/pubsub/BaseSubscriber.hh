//------------------------------------------------------------------------------
// File: BaseSubscriber.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#ifndef QCLIENT_BASE_SUBSCRIBER_HH
#define QCLIENT_BASE_SUBSCRIBER_HH

#include "qclient/QClient.hh"
#include "qclient/Members.hh"
#include "qclient/Options.hh"
#include <set>
#include <mutex>

namespace qclient {

class MessageListener;

//------------------------------------------------------------------------------
//! This is a low-level class, which models closely a redis connection in
//! subscription mode - don't expect a comfortable API.
//!
//! This means we can subscribe into channels and such, while all incoming
//! messages go through a single listener object. We make no effort to filter
//! out the messages according to channel and dispatch accordingly, that's a
//! job for a higher level class.
//------------------------------------------------------------------------------
class BaseSubscriber {
public:
  //----------------------------------------------------------------------------
  //! Constructor taking a list of members for the cluster, the listener, and
  //! options object.
  //!
  //! If you construct a BaseSubscriber with a nullptr listener, we're calling
  //! std::abort. :)
  //----------------------------------------------------------------------------
  BaseSubscriber(const Members &members,
    std::shared_ptr<MessageListener> listener,
    SubscriptionOptions &&options);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~BaseSubscriber();

  //----------------------------------------------------------------------------
  //! Subscribe to the given channels, in addition to any other subscriptions
  //! we may currently have.
  //----------------------------------------------------------------------------
  void subscribe(const std::vector<std::string> &channels);

  //----------------------------------------------------------------------------
  //! Subscribe to the given patterns, in addition to any other subscriptions
  //! we may currently have.
  //----------------------------------------------------------------------------
  void psubscribe(const std::vector<std::string> &patterns);

  //----------------------------------------------------------------------------
  //! Unsubscribe from the given channels. If an empty vector is given, we are
  //! unsubscribed from all channels. (but not patterns!)
  //----------------------------------------------------------------------------
  void unsubscribe(const std::vector<std::string> &channels);

  //----------------------------------------------------------------------------
  //! Unsubscribe from the given patterns. If an empty vector is given, we are
  //! unsubscribed from all patterns. (but not channels!)
  //----------------------------------------------------------------------------
  void punsubscribe(const std::vector<std::string> &patterns);

private:
  //----------------------------------------------------------------------------
  //! Notify of a reconnection in the underlying qclient
  //----------------------------------------------------------------------------
  friend class BaseSubscriberListener;
  std::unique_ptr<ReconnectionListener> reconnectionListener;
  void notifyConnectionEstablished(int64_t epoch);

  Members members;
  std::shared_ptr<MessageListener> listener;
  SubscriptionOptions options;

  std::mutex mtx;
  std::set<std::string> channels;
  std::set<std::string> patterns;
  qclient::QClient qcl;

};

}

#endif