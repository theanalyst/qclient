//------------------------------------------------------------------------------
// File: SharedDeque.hh
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

#ifndef QCLIENT_SHARED_DEQUE_HH
#define QCLIENT_SHARED_DEQUE_HH

#include "qclient/Status.hh"
#include "qclient/ReconnectionListener.hh"
#include <string>
#include <mutex>
#include <memory>

namespace qclient {

class SharedManager;
class QClient;
class Subscription;
class Message;

class SharedDeque final : public ReconnectionListener {
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedDeque(SharedManager *sm, const std::string &key);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~SharedDeque();

  //----------------------------------------------------------------------------
  //! Push an element into the back of the deque
  //----------------------------------------------------------------------------
  qclient::Status push_back(const std::string &contents);

  //----------------------------------------------------------------------------
  //! Clear deque contents
  //----------------------------------------------------------------------------
  qclient::Status clear();

  //----------------------------------------------------------------------------
  //! Remove item from the front of the queue. If queue is empty, "" will be
  //! returned - not an error.
  //----------------------------------------------------------------------------
  qclient::Status pop_front(std::string &out);

  //----------------------------------------------------------------------------
  //! Query deque size
  //----------------------------------------------------------------------------
  qclient::Status size(size_t &out);

  //----------------------------------------------------------------------------
  //! Invalidate cached size
  //----------------------------------------------------------------------------
  void invalidateCachedSize();

  //----------------------------------------------------------------------------
  //! Receive notifications from QClient
  //----------------------------------------------------------------------------
  virtual void notifyConnectionLost(int64_t epoch, int errc, const std::string &msg) override final;
  virtual void notifyConnectionEstablished(int64_t epoch) override final;


private:
  SharedManager *mSharedManager;
  std::string mKey;
  qclient::QClient *mQcl;
  std::unique_ptr<qclient::Subscription> mSubscription;

  std::mutex mCacheMutex;
  size_t mCachedSize = 0u;
  bool mCachedSizeValid = false;

  //----------------------------------------------------------------------------
  //! Process incoming message
  //----------------------------------------------------------------------------
  void processIncoming(Message &&msg);

};

}

#endif
