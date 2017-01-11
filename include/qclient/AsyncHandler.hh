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
//! Enum class OpType - asynchronous requests type that can be handled by the
//! AsyncHandler class
//------------------------------------------------------------------------------
enum class OpType {
  NONE, SADD, SREM, HSET, HDEL, HLEN, DEL
};

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
  //! @param future future object of async request
  //! @param op operation type
  //----------------------------------------------------------------------------
  void Register(std::future<redisReplyPtr>&& future, OpType op);

  //----------------------------------------------------------------------------
  //! Wait for all pending requests and collect the results
  //!
  //! @return true if all successful, otherwise false
  //----------------------------------------------------------------------------
  bool Wait();

  //----------------------------------------------------------------------------
  //! Get responses for async resquests
  //!
  //! @return vector of the responses
  //----------------------------------------------------------------------------
  std::vector<long long int> GetResponses();

private:
  //----------------------------------------------------------------------------
  //! Handle response depending on the operation type
  //!
  //! @param reply redisReply pointer object
  //! @param op_type operation type
  //----------------------------------------------------------------------------
  void HandleResponse(redisReply* reply, OpType op_type);

  std::vector< std::tuple< std::future<redisReplyPtr>, OpType> >
  mVectRequests;
  std::vector<long long int> mVectResponses; ///< Vector of responses
  std::mutex mVectMutex; ///< Mutex protecting access to the vector
  std::list<std::string> mErrors; ///< List of errors
};

QCLIENT_NAMESPACE_END
