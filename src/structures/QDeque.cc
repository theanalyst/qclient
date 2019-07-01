//------------------------------------------------------------------------------
// File: QDeque.cc
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

#include "qclient/structures/QDeque.hh"
#include "qclient/ResponseParsing.hh"
#include "qclient/QClient.hh"

namespace qclient {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QDeque::QDeque(QClient &qcl, const std::string& key)
: mQcl(qcl), mKey(key) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QDeque::~QDeque() {}

//------------------------------------------------------------------------------
// Query deque size
//------------------------------------------------------------------------------
qclient::Status QDeque::size(size_t &out) {
  IntegerParser parser(mQcl.exec("deque-len", mKey).get());
  if(!parser.ok()) {
    return qclient::Status(EINVAL, parser.err());
  }

  out = parser.value();
  return qclient::Status();
}

//------------------------------------------------------------------------------
// Add item to the back of the queue
//------------------------------------------------------------------------------
qclient::Status QDeque::push_back(const std::string &contents) {
  IntegerParser parser(mQcl.exec("deque-push-back", mKey, contents).get());
  if(!parser.ok()) {
    return qclient::Status(EINVAL, parser.err());
  }

  return qclient::Status();
}

//------------------------------------------------------------------------------
// Remove item from the front of the queue. If queue is empty, "" will be
// returned - not an error.
//------------------------------------------------------------------------------
qclient::Status QDeque::pop_front(std::string &out) {
  StringParser parser(mQcl.exec("deque-pop-front", mKey).get());
  if(!parser.ok()) {
    return qclient::Status(EINVAL, parser.err());
  }

  out = parser.value();
  return qclient::Status();
}

//------------------------------------------------------------------------------
// Clear all items in the queue
//------------------------------------------------------------------------------
qclient::Status QDeque::clear() {
  IntegerParser parser(mQcl.exec("deque-clear", mKey).get());
  if(!parser.ok()) {
    return qclient::Status(EINVAL, parser.err());
  }

  return qclient::Status();
}


}
