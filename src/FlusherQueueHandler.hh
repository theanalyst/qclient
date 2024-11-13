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

#ifndef EOS_FLUSHERQUEUEHANDLER_HH
#define EOS_FLUSHERQUEUEHANDLER_HH

#include "qclient/AssistedThread.hh"
#include "qclient/PersistencyLayer.hh"
#include "qclient/QClient.hh"
namespace qclient {

class BackgroundFlusher;
class QClient;
class QCallback;

enum class FlusherQueueHandlerT : uint8_t;

class QueueHandler {
public:
  QueueHandler(BackgroundFlusher * flusher,
               QClient * qclient,
               BackgroundFlusherPersistency * persistency);
  virtual ~QueueHandler() = default;
  virtual void pushRequest(const std::vector<std::string>& operation) = 0;
  virtual void handleAck(ItemIndex index = -1) = 0;
  virtual void restorefromPersistency() = 0;
protected:
  BackgroundFlusher * flusher;
  QClient * qclient;
  BackgroundFlusherPersistency * persistency;
};

struct SerialQueueHandler : public QueueHandler {
  SerialQueueHandler(BackgroundFlusher * flusher,
                     QClient * qclient,
                     BackgroundFlusherPersistency * persistency);
  void pushRequest(const std::vector<std::string>& operation) override;
  void handleAck(ItemIndex) override;
  void restorefromPersistency() override;
private:
  QCallback * callback;
  std::mutex newEntriesMtx;
};

struct LockFreeQueueHandler : public QueueHandler {
  LockFreeQueueHandler(BackgroundFlusher * flusher,
                       QClient * qclient,
                       BackgroundFlusherPersistency * persistency);
  void pushRequest(const std::vector<std::string>& operation) override;
  void handleAck(ItemIndex) override;
  void restorefromPersistency() override;
};

std::unique_ptr<QueueHandler> makeQueueHandler(FlusherQueueHandlerT type);



} // namespace qclient

#endif // EOS_FLUSHERQUEUEHANDLER_HH
