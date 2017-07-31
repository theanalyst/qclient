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
  std::lock_guard<std::mutex> lock(mLstMutex);
  mResponses.clear();

  for (auto& elem: mRequests) {
    try {
      qcl = elem.mClient;
      reply = qcl->HandleResponse(std::move(elem.mAsyncResp));

      if (reply->type == REDIS_REPLY_INTEGER) {
        mResponses.emplace_back(reply->integer);
      } else if (reply->type == REDIS_REPLY_STATUS){
        if (strncmp(reply->str, "OK", 2) == 0) {
          mResponses.emplace_back(1);
        } else {
          std::cerr << "ERROR: REDIS_REPLY_STRING - " << reply->str << std::endl;
          mResponses.emplace_back(-1);
          is_ok &= false;
        }
      } else {
        std::cerr << "ERROR: reply_type: " << reply->type << std::endl;
        mResponses.emplace_back(-EINVAL);
        is_ok &= false;
      }
    } catch (std::runtime_error& qdb_err) {
      std::cerr << "Exception: " << qdb_err.what() << std::endl;
      mResponses.emplace_back(-ECOMM);
      is_ok &= false;
    }
  }

  mRequests.clear();
  return is_ok;
}

//------------------------------------------------------------------------------
// Wait for pending requests and collect the results if there are more than
// required the required number of in-flight requests.
//------------------------------------------------------------------------------
bool
AsyncHandler::WaitForAtLeast(std::uint64_t num_req)
{
  {
    // Wait only if we have enough requests in-flight
    std::lock_guard<std::mutex> lock(mLstMutex);

    if (mRequests.size() <= num_req) {
      return true;
    }
  }

  return Wait();
}

//------------------------------------------------------------------------------
// Get responses for the async requests
//------------------------------------------------------------------------------
std::list<long long int>
AsyncHandler::GetResponses()
{
  std::lock_guard<std::mutex> lock(mLstMutex);
  return mResponses;
}

//------------------------------------------------------------------------------
// Register new future
//------------------------------------------------------------------------------
void
AsyncHandler::Register(qclient::AsyncResponseType&& resp_pair, QClient* qcl)
{
  std::lock_guard<std::mutex> lock(mLstMutex);
  mRequests.emplace_back(std::move(resp_pair), qcl);
}

QCLIENT_NAMESPACE_END
