//------------------------------------------------------------------------------
// File: SharedManager.hh
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

#ifndef QCLIENT_SHARED_MANAGER_HH
#define QCLIENT_SHARED_MANAGER_HH

#include "../AssistedThread.hh"
#include "../Options.hh"
#include <memory>

namespace qclient {

class Members;
class QClient;
class Subscriber;
class TransientSharedHash;

//------------------------------------------------------------------------------
//! SharedManager class to babysit SharedHashes and SharedQueues.
//! Do not destroy this object before all hashes and queues it manages!
//------------------------------------------------------------------------------
class SharedManager {
public:
  //----------------------------------------------------------------------------
  //! Empty constructor, simulation mode.
  //----------------------------------------------------------------------------
  SharedManager();

  //----------------------------------------------------------------------------
  //! Destructor.
  //----------------------------------------------------------------------------
  ~SharedManager();

  //----------------------------------------------------------------------------
  //! Constructor - supply necessary information for connecting to a QDB
  //! instance.
  //! "Options" will be used in a connection publishing information, and
  //! "SubscriptionOptions" in a connection subscribing to the necessary
  //! channels.
  //----------------------------------------------------------------------------
  SharedManager(const qclient::Members &members, qclient::Options &&options,
    qclient::SubscriptionOptions &&subscriptionOptions);

  //----------------------------------------------------------------------------
  //! Make a transient shared hash based on the given channel
  //----------------------------------------------------------------------------
  std::unique_ptr<TransientSharedHash> makeTransientSharedHash(
    const std::string &channel);

  //----------------------------------------------------------------------------
  //! Publish the given message. You probably should not call this directly,
  //! it's used by our dependent shared data structures to publish
  //! modifications.
  //----------------------------------------------------------------------------
  void publish(const std::string &channel, const std::string &payload);

  //----------------------------------------------------------------------------
  //! Get pointer to underlying QClient object - lifetime is tied to this
  //! SharedManager.
  //----------------------------------------------------------------------------
  qclient::QClient* getQClient();

  //----------------------------------------------------------------------------
  //! Get pointer to underlying Subscriber object - lifetime is tied to this
  //! SharedManager.
  //----------------------------------------------------------------------------
  qclient::Subscriber* getSubscriber();

private:
  std::shared_ptr<Logger> logger;
  std::unique_ptr<QClient> qclient;
  std::unique_ptr<Subscriber> subscriber;
};

}

#endif