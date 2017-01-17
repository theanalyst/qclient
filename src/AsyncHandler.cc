//------------------------------------------------------------------------------
//! @file AsyncHandler.cc
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

#include "qclient/AsyncHandler.hh"

QCLIENT_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Wait for all pending requests and collect the results
//------------------------------------------------------------------------------
bool
AsyncHandler::Wait()
{
  bool is_ok = true;
  redisReplyPtr reply;
  QClient* qcl;
  std::lock_guard<std::mutex> lock(mVectMutex);
  mVectResponses.clear();

  for (auto& pair: mVectRequests) {
    try {
      qcl = pair.second;
      reply = qcl->HandleResponse(std::move(pair.first));

      if (reply->type != REDIS_REPLY_INTEGER) {
        mVectResponses.emplace_back(-EINVAL);
        is_ok &= false;
      } else {
        mVectResponses.emplace_back(reply->integer);
      }
    } catch (std::runtime_error& qdb_err) {
      mVectResponses.emplace_back(-ECOMM);
      is_ok &= false;
    }
  }

  mVectRequests.clear();
  return is_ok;
}

//------------------------------------------------------------------------------
// Get responses for the async requests
//------------------------------------------------------------------------------
std::vector<long long int>
AsyncHandler::GetResponses()
{
  std::lock_guard<std::mutex> lock(mVectMutex);
  return mVectResponses;
}

//------------------------------------------------------------------------------
// Register new future
//------------------------------------------------------------------------------
void
AsyncHandler::Register(qclient::AsyncResponseType resp_pair, QClient* qcl)
{
  std::lock_guard<std::mutex> lock(mVectMutex);
  mVectRequests.emplace_back(std::move(resp_pair), qcl);
}

QCLIENT_NAMESPACE_END
