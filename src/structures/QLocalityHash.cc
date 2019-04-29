/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Inspect / change locality hashes
//------------------------------------------------------------------------------

#include "qclient/structures/QLocalityHash.hh"
#include "qclient/QClient.hh"

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl


namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QLocalityHash::Iterator::Iterator(QClient *qcl, const std::string &key, size_t count, const std::string &startCursor)
: mQcl(qcl), mKey(key), mCount(count), mCursor(startCursor) {
  fillFromBackend();
}

//------------------------------------------------------------------------------
// Check if an unexpected error occurred (network issue, data type mismatch)
//------------------------------------------------------------------------------
bool QLocalityHash::Iterator::hasError(std::string &err) const {
  if(mError.empty()) {
    return false;
  }

  err = mError;
  return true;
}

//------------------------------------------------------------------------------
// Report a malformed response
//------------------------------------------------------------------------------
void QLocalityHash::Iterator::malformed(redisReplyPtr reply) {
  mError = SSTR("malformed server response to LHSCAN: "
    << qclient::describeRedisReply(reply));
  return;
}

//------------------------------------------------------------------------------
// Fill internal buffer with contents from remote server
//------------------------------------------------------------------------------
void QLocalityHash::Iterator::fillFromBackend() {
  while(mError.empty() && mResults.empty() && !mReachedEnd) {
    mReqs++;

    redisReplyPtr reply = mQcl->exec("LHSCAN", mKey, mCursor, "COUNT", std::to_string(mCount)).get();

    if(!reply) {
      mError = "unable to contact backend - network error";
      return;
    }

    if(reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
      return malformed(reply);
    }

    redisReply *nextCursor = reply->element[0];
    if(!nextCursor || nextCursor->type != REDIS_REPLY_STRING) {
      return malformed(reply);
    }

    mCursor = std::string(nextCursor->str, nextCursor->len);
    if(mCursor == "0") mReachedEnd = true;

    redisReply *subArray = reply->element[1];
    if(!subArray || subArray->type != REDIS_REPLY_ARRAY || (subArray->elements % 3) != 0) {
      return malformed(reply);
    }

    for(size_t i = 0; i < subArray->elements; i++) {
      redisReply *item = subArray->element[i];
      if(!item || item->type != REDIS_REPLY_STRING) {
        return malformed(reply);
      }

      mResults.emplace_back(item->str, item->len);
    }
  }
}

//------------------------------------------------------------------------------
// Check if the iterator points to a valid entry.
// getKey / getLocalityHint / getValue are possible to call only if
// valid() == true
//------------------------------------------------------------------------------
bool QLocalityHash::Iterator::valid() const {
  return mError.empty() && !mResults.empty();
}

//------------------------------------------------------------------------------
// Fetch current element being pointed to
//------------------------------------------------------------------------------
std::string QLocalityHash::Iterator::getKey() const {
  return mResults[1];
}

std::string QLocalityHash::Iterator::getLocalityHint() const {
  return mResults[0];
}

std::string QLocalityHash::Iterator::getValue() const {
  return mResults[2];
}

//------------------------------------------------------------------------------
// Get total number of network requests this object has issued so far
//------------------------------------------------------------------------------
size_t QLocalityHash::Iterator::requestsSoFar() const {
  return mReqs;
}

//------------------------------------------------------------------------------
// Advance iterator - may result in network requests and block
//------------------------------------------------------------------------------
void QLocalityHash::Iterator::next() {
  if(!mResults.empty()) {
    mResults.erase(mResults.begin(), mResults.begin()+3);
  }

  fillFromBackend();
}

}
