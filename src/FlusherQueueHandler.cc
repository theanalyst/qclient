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
QueueHandler::QueueHandler(BackgroundFlusher* flusher, QClient* qclient,
                           BackgroundFlusherPersistency* persistency)
    : flusher(flusher), qclient(qclient), persistency(persistency)
{
}

SerialQueueHandler::SerialQueueHandler(BackgroundFlusher* parent, QClient* qclient,
                                       BackgroundFlusherPersistency* persistency)
    : QueueHandler(parent, qclient, persistency)
{
}
void
SerialQueueHandler::pushRequest(const std::vector<std::string>& operation)
{
  std::lock_guard lock(newEntriesMtx);
  persistency->record(persistency->getEndingIndex(), operation);
  qclient->execute(callback, operation);
}

void
SerialQueueHandler::handleAck(ItemIndex)
{
  {
    std::lock_guard lock(newEntriesMtx);
    persistency->pop();
  }
  flusher->notifyWaiters();
}

void
SerialQueueHandler::restorefromPersistency()
{
  for (ItemIndex i = persistency->getStartingIndex();
       i != persistency->getEndingIndex(); i++) {
    std::vector<std::string> contents;
    if (!persistency->retrieve(i, contents)) {
      std::cerr << "BackgroundFlusher corruption, could not retrieve entry with index "
                << i << std::endl;
      std::terminate();
    }
    qclient->execute(callback, contents);
  }
}

LockFreeQueueHandler::LockFreeQueueHandler(BackgroundFlusher* _parent, QClient* _qclient, BackgroundFlusherPersistency* _persistency)
    : QueueHandler(_parent, _qclient, _persistency)
{
}

void
LockFreeQueueHandler::pushRequest(const std::vector<std::string>& operation)
{
  auto index = persistency->record(operation);
  qclient->execute(new BackgroundFlusher::StatefulCallback(flusher, index), operation);
}

void
LockFreeQueueHandler::handleAck(ItemIndex index)
{
  persistency->popIndex(index);
  flusher->notifyWaiters();
}

void
LockFreeQueueHandler::restorefromPersistency()
{
  for (ItemIndex i = persistency->getStartingIndex();
       i != persistency->getEndingIndex(); i++) {
    std::vector<std::string> contents;
    if (!persistency->retrieve(i, contents)) {
      std::cerr << "Skipping item at index=" << i << std::endl;
      continue;
    }
    qclient->execute(new BackgroundFlusher::StatefulCallback(flusher, i), contents);
  }
}



