//------------------------------------------------------------------------------
// File: UpdateBatch.hh
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

#ifndef QCLIENT_UPDATE_BATCH_HH
#define QCLIENT_UPDATE_BATCH_HH

#include <map>
#include <string>

namespace qclient {

class UpdateBatch {
public:
  using ConstIterator = std::map<std::string, std::string>::const_iterator;

  //----------------------------------------------------------------------------
  //! Set durable value
  //----------------------------------------------------------------------------
  void setDurable(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Set transient value
  //----------------------------------------------------------------------------
  void setTransient(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Set local value
  //----------------------------------------------------------------------------
  void setLocal(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Durable updates iterators
  //----------------------------------------------------------------------------
  ConstIterator durableBegin() const;
  ConstIterator durableEnd() const;

  //----------------------------------------------------------------------------
  //! Transient updates iterators
  //----------------------------------------------------------------------------
  ConstIterator transientBegin() const;
  ConstIterator transientEnd() const;

  //----------------------------------------------------------------------------
  //! Local updates iterators
  //----------------------------------------------------------------------------
  ConstIterator localBegin() const;
  ConstIterator localEnd() const;

private:
  std::map<std::string, std::string> mDurableUpdates;
  std::map<std::string, std::string> mTransientUpdates;
  std::map<std::string, std::string> mLocalUpdates;
};

}

#endif
