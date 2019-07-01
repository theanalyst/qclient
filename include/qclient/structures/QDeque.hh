//------------------------------------------------------------------------------
// File: QDeque.hh
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

#ifndef QCLIENT_DEQUE_HH
#define QCLIENT_DEQUE_HH

#include <string>
#include <qclient/Status.hh>

namespace qclient {

class QClient;

//------------------------------------------------------------------------------
//! Class QDeque: All operations are synchronous.
//------------------------------------------------------------------------------
class QDeque {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QDeque(QClient &qcl, const std::string& key);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QDeque();

  //----------------------------------------------------------------------------
  //! Query deque size
  //----------------------------------------------------------------------------
  qclient::Status size(size_t &out);

  //----------------------------------------------------------------------------
  //! Add item to the back of the queue
  //----------------------------------------------------------------------------
  qclient::Status push_back(const std::string &contents);

  //----------------------------------------------------------------------------
  //! Remove item from the front of the queue. If queue is empty, "" will be
  //! returned - not an error.
  //----------------------------------------------------------------------------
  qclient::Status pop_front(std::string &out);

  //----------------------------------------------------------------------------
  //! Clear all items in the queue
  //----------------------------------------------------------------------------
  qclient::Status clear();

private:
  qclient::QClient& mQcl;
  std::string mKey;
};

}

#endif
