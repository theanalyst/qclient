//------------------------------------------------------------------------------
// File: SharedHash.hh
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

#ifndef QCLIENT_SHARED_HASH_HH
#define QCLIENT_SHARED_HASH_HH

#include <map>
#include <mutex>

namespace qclient {

//------------------------------------------------------------------------------
//! A "shared hash" class which uses pubsub messages and versioned hashes to
//! synchronize the contents of a hash between multiple clients.
//!
//! It has the following characteristics:
//! - Contents are always persisted.
//! - QuarkDB is the "single source of truth" - local contents will always be
//!   overwritten depending on what is on QDB.
//! - Eventually consistent - if there are network instabilities, it could
//!   get out of sync to the "true" state stored in QDB, but eventually, it
//!   _will_ be consistent.
//! - Not a great idea to store many items in this hash - we regularly need
//!   to fetch the entire contents and overwrite the local map in case of
//!   network instabilities.
//------------------------------------------------------------------------------
class SharedManager;

class SharedHash {
public:
  //----------------------------------------------------------------------------
  //! Constructor - supply a SharedManager object. I'll keep a reference to it
  //! throughout my lifetime - don't destroy it before me!
  //----------------------------------------------------------------------------
  SharedHash(SharedManager *sm, const std::string &key);

  //----------------------------------------------------------------------------
  //! Read contents of the specified field.
  //! Returns true if found, false otherwise.
  //----------------------------------------------------------------------------
  bool get(const std::string &field, std::string& value);

  //----------------------------------------------------------------------------
  //! Set contents of the specified field.
  //! Not guaranteed to succeed in case of network instabilities.
  //----------------------------------------------------------------------------
  void set(const std::string &field, const std::string &value);

  //----------------------------------------------------------------------------
  //! Delete the specified field.
  //! Not guaranteed to succeed in case of network instabilities.
  //----------------------------------------------------------------------------
  void del(const std::string &field);




private:
  std::string key;
  SharedManager *sm;

  std::map<std::string, std::string> contents;
  std::mutex contentMutex;

};

}

#endif

