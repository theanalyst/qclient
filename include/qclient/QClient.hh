//------------------------------------------------------------------------------
// File: QClient.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef QCLIENT_QCLIENT_H
#define QCLIENT_QCLIENT_H

#include <mutex>
#include <future>
#include <queue>
#include <map>
#include <list>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include "qclient/TlsFilter.hh"
#include "qclient/EventFD.hh"
#include "qclient/Members.hh"
#include "qclient/Utils.hh"
#include "qclient/QCallback.hh"
#include "qclient/Options.hh"
#include "qclient/Handshake.hh"
#include "qclient/EncodedRequest.hh"
#include "qclient/ResponseBuilder.hh"
#include "qclient/AssistedThread.hh"
#include "qclient/FaultInjector.hh"

#if HAVE_FOLLY == 1
#include <folly/futures/Future.h>
#endif

//------------------------------------------------------------------------------
//! Instantiate a few templates inside a single, internal compilation unit,
//! to save compile time. The alternative is to have every single compilation
//! unit which includes QClient.hh instantiate them, which increases compilation
//! time.
//------------------------------------------------------------------------------
extern template class std::future<qclient::redisReplyPtr>;
#if HAVE_FOLLY == 1
extern template class folly::Future<qclient::redisReplyPtr>;
#endif

namespace qclient
{
  class QCallback;
  class NetworkStream;
  class WriterThread;
  class ConnectionCore;
  class EndpointDecider;
  class HostResolver;

//------------------------------------------------------------------------------
//! Describe a redisReplyPtr, in a format similar to what redis-cli would give.
//------------------------------------------------------------------------------
std::string describeRedisReply(const redisReply *const redisReply, const std::string &prefix = "");
std::string describeRedisReply(const redisReplyPtr &redisReply);

//------------------------------------------------------------------------------
//! Class QClient
//------------------------------------------------------------------------------
class QClient
{
public:
  //----------------------------------------------------------------------------
  //! Constructor taking simple host and port
  //----------------------------------------------------------------------------
  QClient(const std::string &host, int port, Options &&options);

  //----------------------------------------------------------------------------
  //! Constructor taking a list of members for the cluster
  //----------------------------------------------------------------------------
  QClient(const Members &members, Options &&options);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QClient();

  //----------------------------------------------------------------------------
  //! Disallow copy and assign
  //----------------------------------------------------------------------------
  QClient(const QClient&) = delete;
  void operator=(const QClient&) = delete;

  //----------------------------------------------------------------------------
  //! Primary execute command that takes an EncodedRequest and sends it
  //! over the network
  //!
  //! @return future holding a redis reply
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> execute(EncodedRequest &&req);
  void execute(QCallback *callback, EncodedRequest &&req);
#if HAVE_FOLLY == 1
  folly::Future<redisReplyPtr> follyExecute(EncodedRequest &&req);
#endif

  //----------------------------------------------------------------------------
  //! Execute multiple commands in a MULTI / EXEC transaction. Retries will
  //! work as expected: If the connection dies in the middle, the whole block
  //! will be retried from start.
  //!
  //! It's also safe to issue multiple transactions in parallel on the same
  //! QClient, as we serialize them and make sure they don't overlap.
  //!
  //! QUEUED intermediate responses are abstracted away, we only return the
  //! result of "EXEC".
  //!
  //! NOTE: Don't insert MULTI / EXEC, it's done automatically for you.
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> execute(std::deque<EncodedRequest> &&reqs);
  void execute(QCallback *callback, std::deque<EncodedRequest> &&req);
#if HAVE_FOLLY == 1
  folly::Future<redisReplyPtr> follyExecute(std::deque<EncodedRequest> &&req);
#endif

  //----------------------------------------------------------------------------
  //! Conveninence function to encode a redis command given as a container of
  //! strings to a redis buffer
  //!
  //! @param T container: must implement begin() and end()
  //!
  //! @return future object
  //----------------------------------------------------------------------------
  template<typename T>
  std::future<redisReplyPtr> execute(const T& container) {
    return execute(EncodedRequest(container));
  }

  template<typename T>
  void execute(QCallback *callback, const T& container) {
    return execute(callback, EncodedRequest(container));
  }

#if HAVE_FOLLY == 1
  template<typename T>
  folly::Future<redisReplyPtr> follyExecute(const T& container) {
    return follyExecute(EncodedRequest(container));
  }
#endif

  //----------------------------------------------------------------------------
  // Convenience function, used mainly in tests.
  // This makes it possible to call exec("get", "key") instead of having to
  // build a vector.
  //
  // Extremely useful in macros, which don't support universal initialization.
  //----------------------------------------------------------------------------
  template<typename... Args>
  std::future<redisReplyPtr> exec(const Args&... args) {
    return this->execute(EncodedRequest::make(args...));
  }

  //----------------------------------------------------------------------------
  // The same as the above, but takes a callback instead of return a future.
  // Different name, as overloading with a variadic template is a bad idea.
  //----------------------------------------------------------------------------
  template<typename... Args>
  void execCB(QCallback *callback, const Args... args) {
    return this->execute(callback, std::vector<std::string> {args...});
  }

  //----------------------------------------------------------------------------
  // The same as the above, but returns a folly future.
  //----------------------------------------------------------------------------
#if HAVE_FOLLY == 1
  template<typename... Args>
  folly::Future<redisReplyPtr> follyExec(const Args... args) {
    return this->follyExecute(std::vector<std::string> {args...});
  }
#endif

  //----------------------------------------------------------------------------
  //! Return fault injector object for this QClient
  //----------------------------------------------------------------------------
  FaultInjector& getFaultInjector();

  //----------------------------------------------------------------------------
  //! Wrapper function for exists command
  //!
  //! @param key key to search for
  //!
  //! @return 1 if key exists, 0 if it doesn't, -errno if any error occured
  //----------------------------------------------------------------------------
  long long int
  exists(const std::string& key);

  //----------------------------------------------------------------------------
  //! Wrapper function for del command
  //!
  //! @param key key to be deleted
  //!
  //! @return number of keys deleted, -errno if any error occured
  //----------------------------------------------------------------------------
  long long int
  del(const std::string& key);

private:
  // The cluster members, as given in the constructor.
  Members members;

  std::unique_ptr<EndpointDecider> endpointDecider;

  // the endpoint we're actually connecting to
  Options options;

  std::chrono::steady_clock::time_point lastAvailable;
  bool successfulResponses = false;
  bool successfulResponsesEver = false;
  std::unique_ptr<NetworkStream> networkStream;

  void startEventLoop();
  void eventLoop(ThreadAssistant &assistant);
  void connect();
  bool handleConnectionEpoch(ThreadAssistant &assistant);
  bool shouldPurgePendingRequests();
  ResponseBuilder responseBuilder;
  int64_t currentConnectionEpoch = 0u;

  void cleanup(bool shutdown);
  bool feed(const char* buf, size_t len);
  void connectTCP();
  void notifyConnectionLost(int errc, const std::string &err);
  void notifyConnectionEstablished();

  std::unique_ptr<ConnectionCore> connectionCore;
  EventFD shutdownEventFD;
  std::unique_ptr<WriterThread> writerThread;

  void processRedirection();
  AssistedThread eventLoopThread;
  FaultInjector faultInjector;

  friend class FaultInjector;

  std::unique_ptr<HostResolver> hostResolver;

  //----------------------------------------------------------------------------
  // Notify this QClient object that a fault injection has been added
  //----------------------------------------------------------------------------
  void notifyFaultInjectionsUpdated();
};

}

#endif
