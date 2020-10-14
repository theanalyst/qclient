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

#ifdef EOSCITRINE
#include "common/SharedMutexWrapper.hh"
#else
#include <shared_mutex>
#endif


namespace qclient {

struct CommunicatorReply {
  int64_t status;
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

  struct Item {
    Item() {}
    Item(const RequestID &reqid) : id(reqid) {}

    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point lastRetry;

    RequestID id;

    std::string channel;
    std::string contents;
    std::promise<CommunicatorReply> promise;

    std::list<RequestID>::iterator listIter;
  };

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
  size_t size() const;

  //----------------------------------------------------------------------------
  // Get earliest retry
  // - Return value False: Vault is empty
  // - Return value True: tp is filled with earliest lastRetry time_point
  //----------------------------------------------------------------------------
  bool getEarliestRetry(std::chrono::steady_clock::time_point &tp);

  //----------------------------------------------------------------------------
  // Block until there's an item in the queue
  //----------------------------------------------------------------------------
  void blockUntilNonEmpty();

  //----------------------------------------------------------------------------
  // Set blocking mode
  //----------------------------------------------------------------------------
  void setBlockingMode(bool val);

  //----------------------------------------------------------------------------
  // Expire any items which were submitted past the deadline.
  // Only the original submission time counts here, not the retries.
  //----------------------------------------------------------------------------
  size_t expire(std::chrono::steady_clock::time_point deadline);

  //----------------------------------------------------------------------------
  // Retry front item, if it exists
  //----------------------------------------------------------------------------
  bool retryFrontItem(std::chrono::steady_clock::time_point now,
    std::string &channel, std::string &contents, std::string &id);

private:
  //----------------------------------------------------------------------------
  // Drop front item
  //----------------------------------------------------------------------------
  void dropFront();

  using PendingRequestMap = std::map<RequestID, Item>;

  PendingRequestMap mPendingRequests;
  std::list<RequestID> mNextToRetry;
  bool mBlockingMode {true};

  mutable std::mutex mMutex;
  std::condition_variable mCV;

};

}

#endif
