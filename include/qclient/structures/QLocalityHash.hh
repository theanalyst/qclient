//------------------------------------------------------------------------------
// File: QLocalityHash.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef QCLIENT_STRUCTURES_LOCALITY_HASH_HH
#define QCLIENT_STRUCTURES_LOCALITY_HASH_HH

#include <string>
#include <deque>
#include "qclient/Reply.hh"

namespace qclient {

class QClient;

//------------------------------------------------------------------------------
//! Class QLocalityHash
//------------------------------------------------------------------------------
class QLocalityHash
{
public:

  //----------------------------------------------------------------------------
  //! Iterator class
  //----------------------------------------------------------------------------
  class Iterator {
  public:
    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    Iterator(QClient *qcl, const std::string &key, size_t count = 100000,
      const std::string &startCursor = "0");

    //--------------------------------------------------------------------------
    //! Fetch current element being pointed to
    //--------------------------------------------------------------------------
    std::string getKey() const;
    std::string getLocalityHint() const;
    std::string getValue() const;

    //--------------------------------------------------------------------------
    //! Check if an unexpected error occurred (network issue,
    //! data type mismatch)
    //--------------------------------------------------------------------------
    bool hasError(std::string &out) const;

    //--------------------------------------------------------------------------
    //! Check if the iterator points to a valid entry.
    //! getKey / getLocalityHint / getValue are possible to call only if
    //! valid() == true
    //--------------------------------------------------------------------------
    bool valid() const;

    //--------------------------------------------------------------------------
    //! Advance iterator - may result in network requests and block
    //--------------------------------------------------------------------------
    void next();

    //--------------------------------------------------------------------------
    //! Get total number of network requests this object has issued so far
    //------------------------------------------------------------------------------
    size_t requestsSoFar() const;

  private:
    //--------------------------------------------------------------------------
    //! Report a malformed response
    //--------------------------------------------------------------------------
    void malformed(redisReplyPtr reply);

    //--------------------------------------------------------------------------
    //! Fill internal buffer with contents from remote server
    //--------------------------------------------------------------------------
    void fillFromBackend();

    QClient *mQcl;
    std::string mKey;

    uint32_t mCount;
    std::string mCursor;
    bool mReachedEnd = false;
    size_t mReqs = 0;

    std::deque<std::string> mResults;
    redisReplyPtr mReply;

    std::string mError;
  };

};

}

#endif
