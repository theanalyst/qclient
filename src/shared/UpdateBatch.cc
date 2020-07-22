//------------------------------------------------------------------------------
// File: UpdateBatch.cc
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

#include "qclient/shared/UpdateBatch.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Set durable value
//------------------------------------------------------------------------------
void UpdateBatch::setDurable(const std::string& key, const std::string& value) {
  mDurableUpdates[key] = value;
}

//------------------------------------------------------------------------------
//! Set transient value
//------------------------------------------------------------------------------
void UpdateBatch::setTransient(const std::string& key, const std::string& value) {
  mTransientUpdates[key] = value;
}

//------------------------------------------------------------------------------
//! Set local value
//------------------------------------------------------------------------------
void UpdateBatch::setLocal(const std::string& key, const std::string& value) {
  mLocalUpdates[key] = value;
}

//------------------------------------------------------------------------------
//! Durable updates iterators
//------------------------------------------------------------------------------
UpdateBatch::ConstIterator UpdateBatch::durableBegin() const {
  return mDurableUpdates.begin();
}

UpdateBatch::ConstIterator UpdateBatch::durableEnd() const {
  return mDurableUpdates.end();
}

//------------------------------------------------------------------------------
//! Transient updates iterators
//------------------------------------------------------------------------------
UpdateBatch::ConstIterator UpdateBatch::transientBegin() const {
  return mTransientUpdates.begin();
}

UpdateBatch::ConstIterator UpdateBatch::transientEnd() const {
  return mTransientUpdates.end();
}

//------------------------------------------------------------------------------
//! Local updates iterators
//------------------------------------------------------------------------------
UpdateBatch::ConstIterator UpdateBatch::localBegin() const {
  return mLocalUpdates.begin();
}

UpdateBatch::ConstIterator UpdateBatch::localEnd() const {
  return mLocalUpdates.end();
}

}
