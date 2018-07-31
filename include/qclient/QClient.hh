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
  class ConnectionHandler;


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
  std::future<redisReplyPtr> exec(const Args... args) {
    return this->execute(std::vector<std::string> {args...});
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

  //----------------------------------------------------------------------------
  //! Wrapper function for del async command
  //!
  //! @param key key to be deleted
  //!
  //! @return future object containing the response and the command
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr>
  del_async(const std::string& key);


private:
  // The cluster members, as given in the constructor.
  size_t nextMember = 0;
  Members members;

  // the endpoint we're actually connecting to
  Endpoint targetEndpoint;

  // the endpoint given in a redirect
  Endpoint redirectedEndpoint;

  Options options;

  bool redirectionActive = false;
  std::chrono::steady_clock::time_point lastAvailable;
  bool successfulResponses;

  std::unique_ptr<NetworkStream> networkStream;
  std::atomic<int64_t> shutdown {false};

  void startEventLoop();
  void eventLoop();
  void connect();
  bool shouldPurgePendingRequests();
  ResponseBuilder responseBuilder;

  void cleanup();
  bool feed(const char* buf, size_t len);
  void connectTCP();

  std::unique_ptr<WriterThread> writerThread;
  std::unique_ptr<ConnectionHandler> connectionHandler;
  EventFD shutdownEventFD;

  void primeConnection();
  void processRedirection();
  std::thread eventLoopThread;
};

}

#endif
