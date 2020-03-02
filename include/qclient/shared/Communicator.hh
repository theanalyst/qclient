//------------------------------------------------------------------------------
// File: Communicator.hh
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

#ifndef QCLIENT_SHARED_COMMUNICATOR_HH
#define QCLIENT_SHARED_COMMUNICATOR_HH

#include "qclient/shared/PendingRequestVault.hh"
#include "qclient/AssistedThread.hh"

namespace qclient {

class Subscriber;
class Subscription;
class QClient;
class Message;
class SteadyClock;

//------------------------------------------------------------------------------
// Convenience class for point-to-point request / response messaging between
// two clients with QuarkDB acting as the middleman.
//
// Implements proper retries, backoff, and timeouts. Requires an ACK from
// the other side, which can optionally send a status code and a message.
//
// We need this for legacy reasons, if you're designing a system from scratch
// I'm not sure how reasonable doing this would be. It could be useful for
// very infrequent messages.
//
// For high volume messages, direct point-to-point with a TCP connection would
// always be better than this contraption.
//
// The Communicator class is used for sending messages only. To receive them
// from the other side, use CommunicatorListener.
//------------------------------------------------------------------------------
class Communicator {
public:
  //----------------------------------------------------------------------------
  // Convenience class for point-to-point request / response messaging
  //----------------------------------------------------------------------------
  Communicator(Subscriber* subscriber, const std::string &channel, SteadyClock* clock = nullptr,
    std::chrono::milliseconds mRetryInterval = std::chrono::seconds(10),
    std::chrono::seconds mHardDeadline = std::chrono::seconds(60)
  );

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~Communicator();

  //----------------------------------------------------------------------------
  // Issue a request on the given channel, retrieve assigned ID
  //----------------------------------------------------------------------------
  std::future<CommunicatorReply> issue(const std::string &contents, std::string &id);

  //----------------------------------------------------------------------------
  // Issue a request on the given channel
  //----------------------------------------------------------------------------
  std::future<CommunicatorReply> issue(const std::string &contents);

  //----------------------------------------------------------------------------
  // Run next-to-retry pass
  //
  // Return value:
  // - False: Nothing to retry
  // - True: We have something to retry
  //----------------------------------------------------------------------------
  bool runNextToRetry(std::string &channel, std::string &contents, std::string &id);

  //----------------------------------------------------------------------------
  // Get time to sleep until next retry
  //----------------------------------------------------------------------------
  std::chrono::milliseconds getSleepUntilRetry() const;

private:
  //----------------------------------------------------------------------------
  // Cleanup and retry thread
  //----------------------------------------------------------------------------
  void backgroundThread(ThreadAssistant &assistant);

  //----------------------------------------------------------------------------
  // Process incoming message
  //----------------------------------------------------------------------------
  void processIncoming(Message &&msg);

  Subscriber* mSubscriber;
  std::string mChannel;
  SteadyClock *mClock;

  QClient* mQcl;
  PendingRequestVault mPendingVault;
  std::unique_ptr<Subscription> mSubscription;

  std::chrono::milliseconds mRetryInterval;
  std::chrono::milliseconds mHardDeadline;

  AssistedThread mThread;

};

}

#endif
