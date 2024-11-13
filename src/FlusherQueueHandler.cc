// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2024 CERN/Switzerland                           *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************
//

#include "FlusherQueueHandler.hh"
#include "qclient/BackgroundFlusher.hh"

using namespace qclient;

SerialQueueHandler::SerialQueueHandler(BackgroundFlusher* parent)
    : QueueHandler(parent), callback(&parent->callback)
{
}
void
SerialQueueHandler::pushRequest(const std::vector<std::string>& operation)
{
  std::lock_guard lock(newEntriesMtx);
  parent->persistency->record(parent->persistency->getEndingIndex(), operation);
  parent->qclient->execute(callback, operation);
}

void
SerialQueueHandler::handleAck(ItemIndex)
{
  {
    std::lock_guard lock(newEntriesMtx);
    parent->persistency->pop();
  }
  parent->notifyWaiters();
}

void
SerialQueueHandler::restorefromPersistency()
{
  for (ItemIndex i = parent->persistency->getStartingIndex();
       i != parent->persistency->getEndingIndex(); i++) {
    std::vector<std::string> contents;
    if (!parent->persistency->retrieve(i, contents)) {
      std::cerr << "BackgroundFlusher corruption, could not retrieve entry with index "
                << i << std::endl;
      std::terminate();
    }
    parent->qclient->execute(callback, contents);
  }
}

LockFreeQueueHandler::LockFreeQueueHandler(BackgroundFlusher* _parent)
    : QueueHandler(_parent)
{
}

void
LockFreeQueueHandler::pushRequest(const std::vector<std::string>& operation)
{
  auto index = parent->persistency->record(operation);
  parent->qclient->execute(new StatefulCallback(parent, index), operation);
}

void
LockFreeQueueHandler::handleAck(ItemIndex index)
{
  parent->persistency->popIndex(index);
  parent->notifyWaiters();
}

void
LockFreeQueueHandler::restorefromPersistency()
{
  for (ItemIndex i = parent->persistency->getStartingIndex();
       i != parent->persistency->getEndingIndex(); i++) {
    std::vector<std::string> contents;
    if (!parent->persistency->retrieve(i, contents)) {
      std::cerr << "Skipping item at index=" << i << std::endl;
      continue;
    }
    parent->qclient->execute(new StatefulCallback(parent, i), contents);
  }
}



