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
#include "qclient/QClient.hh"
#include <iostream>

QCLIENT_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Wait for all pending requests and collect the results
//------------------------------------------------------------------------------
bool
AsyncHandler::Wait()
{
  bool is_ok = true;
  redisReplyPtr reply;
  std::lock_guard<std::mutex> lock(mLstMutex);
  mResponses.clear();

  for (auto& elem: mRequests) {
    try {
      reply = elem.mAsyncResp.get();

      if (reply == nullptr) {
        throw std::runtime_error("[FATAL] Error request could not be sent to "
                                 "the QuarkDB backend");
      }

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
AsyncHandler::Register(QClient* qcl, const std::vector<std::string>& cmd)
{
  std::future<redisReplyPtr> reply = qcl->execute(cmd);
  std::lock_guard<std::mutex> lock(mLstMutex);
  mResponses.clear();
  mRequests.emplace_back(qcl, std::move(reply));
}

QCLIENT_NAMESPACE_END
