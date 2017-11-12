//------------------------------------------------------------------------------
//! @file AsyncHandler.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
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

#pragma once
#include "qclient/Namespace.hh"
#include "qclient/QSet.hh"
#include "qclient/QHash.hh"
#include <list>

QCLIENT_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class AsyncHandler
//------------------------------------------------------------------------------
class AsyncHandler
{
public:

  //----------------------------------------------------------------------------
  //! Default constructor
  //----------------------------------------------------------------------------
  AsyncHandler() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AsyncHandler() = default;

  //----------------------------------------------------------------------------
  //! Register a new async call
  //!
  //! @param resp_pair pair containing the furture object and the command it
  //!        corresponds to
  //! @param qcl pointer to client object used to send the request. This is
  //!        used in case the recovery mechanism is triggered.
  //----------------------------------------------------------------------------
  void Register(std::future<redisReplyPtr>&& resp_pair, QClient* qcl);

  //----------------------------------------------------------------------------
  //! Wait for all pending requests and collect the results
  //!
  //! @return true if all successful, otherwise false
  //----------------------------------------------------------------------------
  bool Wait();

  //----------------------------------------------------------------------------
  //! Get responses for async resquests
  //!
  //! @return list of responses
  //----------------------------------------------------------------------------
  std::list<long long int> GetResponses();

private:
  //! Pairs of std::future<redisReplyPtr> and pointer to the qclient object
  //! used to send the request
  struct ReqType {
    std::future<redisReplyPtr> mAsyncResp;
    QClient* mClient;

    ReqType(std::future<redisReplyPtr>&& aresp, QClient* client):
      mAsyncResp(std::move(aresp)), mClient(client) {}
  };

  std::list<ReqType> mRequests; ///< List of executed requests
  std::list<long long int> mResponses; ///< List of responses
  std::mutex mLstMutex; ///< Mutex protecting access to the list
  std::list<std::string> mErrors; ///< List of errors
};

QCLIENT_NAMESPACE_END
