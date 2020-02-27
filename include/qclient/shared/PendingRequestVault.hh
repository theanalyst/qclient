//------------------------------------------------------------------------------
// File: PendingRequestVault.hh
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

#ifndef QCLIENT_SHARED_PENDING_REQUEST_VAULT_HH
#define QCLIENT_SHARED_PENDING_REQUEST_VAULT_HH

#include <string>
#include <future>
#include <map>
#include <chrono>
#include <list>
#include <shared_mutex>
#include "qclient/queueing/WaitableQueue.hh"

namespace qclient {

struct CommunicatorReply {
  int status;
  std::string contents;
};

//------------------------------------------------------------------------------
// Structure to keep track of current pending requests, and provide easy access
// to ones that need to be retried.
//
// Operations:
// - Insert a new request, provide a UUID and std::future<Reply>
//------------------------------------------------------------------------------
class PendingRequestVault {
public:
  using RequestID = std::string;

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  PendingRequestVault();

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~PendingRequestVault();

  //----------------------------------------------------------------------------
  // Insert pending request
  //----------------------------------------------------------------------------
  struct InsertOutcome {
    RequestID id;
    std::future<CommunicatorReply> fut;
  };

  InsertOutcome insert(const std::string &channel, const std::string &contents,
    std::chrono::steady_clock::time_point timepoint);

  //----------------------------------------------------------------------------
  // Satisfy pending request
  //----------------------------------------------------------------------------
  bool satisfy(const RequestID &id, CommunicatorReply &&reply);

  //----------------------------------------------------------------------------
  // Get current pending requests
  //----------------------------------------------------------------------------
  bool size() const;


private:
  struct Item {
    Item(const RequestID &reqid) : id(reqid) {}

    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point lastRetry;

    RequestID id;

    std::string channel;
    std::string contents;
    std::promise<CommunicatorReply> promise;

    std::list<RequestID>::iterator listIter;
  };

  using PendingRequestMap = std::map<RequestID, Item>;

  PendingRequestMap mPendingRequests;
  std::list<RequestID> mNextToRetry;
  mutable std::shared_timed_mutex mMutex;

};

}

#endif
