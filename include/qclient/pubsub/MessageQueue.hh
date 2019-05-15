// ----------------------------------------------------------------------
// File: MessageQueue.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#ifndef QCLIENT_PUBSUB_MESSAGE_QUEUE_HH
#define QCLIENT_PUBSUB_MESSAGE_QUEUE_HH

#include "qclient/queueing/WaitableQueue.hh"
#include "qclient/MessageListener.hh"

namespace qclient {

class MessageQueue : public MessageListener {
public:
  using Iterator = WaitableQueue<Message, 100>::Iterator;

  virtual void handleIncomingMessage(Message&& msg) override {
    queue.emplace_back(std::move(msg));
  }

  void setBlockingMode(bool value) {
    queue.setBlockingMode(value);
  }

  void pop_front() {
    queue.pop_front();
  }

  Iterator begin() {
    return queue.begin();
  }

  size_t size() const {
    return queue.size();
  }

  void clear() {
    queue.reset();
  }

private:
  qclient::WaitableQueue<Message, 100> queue;
};


}

#endif
