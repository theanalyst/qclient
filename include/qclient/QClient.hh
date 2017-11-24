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

#ifndef __QCLIENT_QCLIENT_H__
#define __QCLIENT_QCLIENT_H__

#include <sys/eventfd.h>
#include <mutex>
#include <future>
#include <queue>
#include <map>
#include <list>
#include <hiredis/hiredis.h>
#include <unistd.h>
#include <iostream>
#include <string.h>
#include "qclient/TlsFilter.hh"
#include "qclient/EventFD.hh"
#include "qclient/Members.hh"

namespace qclient
{
  class NetworkStream;
  using redisReplyPtr = std::shared_ptr<redisReply>;

//------------------------------------------------------------------------------
//! Class handshake - inherit from here.
//! Defines the first ever request to send to the remote host, and validates
//! the response. If response is not as expected, the connection is shut down.
//------------------------------------------------------------------------------
class Handshake
{
public:
  virtual ~Handshake() {}
  virtual std::vector<std::string> provideHandshake() = 0;
  virtual bool validateResponse(const redisReplyPtr &reply) = 0;
};

//------------------------------------------------------------------------------
//! Class QClient
//------------------------------------------------------------------------------
class QClient
{
public:
  QClient(const std::string &host, int port, bool redirects = false, bool exceptions = false,
          TlsConfig tlsconfig = {}, std::unique_ptr<Handshake> handshake = {} );

  QClient(const Members &members, bool redirects = false, bool exceptions = false,
          TlsConfig tlsconfig = {}, std::unique_ptr<Handshake> handshake = {} );

  ~QClient();

  // Disallow copy and assign
  QClient(const QClient&) = delete;
  void operator=(const QClient&) = delete;

  std::future<redisReplyPtr> execute(const std::vector<std::string>& req) {
    return execute(req.begin(), req.end());
  }
  std::future<redisReplyPtr> execute(const char* buffer, size_t len);
  std::future<redisReplyPtr> execute(size_t nchunks, const char** chunks,
                                     const size_t* sizes);

  //----------------------------------------------------------------------------
  //! Execute a command given by the begin and end iterator to a container of
  //! strings.
  //!
  //! @param begin iterator pointing to the beginning of the container
  //! @param end iterator pionting to the end of the end of the container
  //!
  //! @return future object
  //----------------------------------------------------------------------------
  template<typename Iterator>
  std::future<redisReplyPtr> execute(const Iterator& begin, const Iterator& end);

  //----------------------------------------------------------------------------
  //! Execute a command provided by a container
  //!
  //! @param T container: must implement begin() and end()
  //!
  //! @return future object
  //----------------------------------------------------------------------------
  template<typename T>
  std::future<redisReplyPtr> execute(const T& container) {
    return this->execute(container.begin(), container.end());
  }

  //----------------------------------------------------------------------------
  // Convenience function, used mainly in tests.
  // This makes it possible to call exec("get", "key") instead of having to
  // build a vector.
  //
  // Extremely useful in macros, which don't support universal initialization.
  //----------------------------------------------------------------------------
  template<typename... Args>
  std::future<redisReplyPtr> exec(const Args... args)
  {
    return this->execute(std::vector<std::string> {args...});
  }

  //----------------------------------------------------------------------------
  // Slight hack needed for unit tests. After an intercept has been added, any
  // connections to (host, ip) will be redirected to (host2, ip2) - usually
  // localhost.
  //----------------------------------------------------------------------------
  static void addIntercept(const std::string& host, const int port,
                           const std::string& host2, const int port2);
  static void clearIntercepts();

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

  //----------------------------------------------------------------------------
  //! Handle response. There are several scenarios to handle:
  //! 1. REDIS_REPLY_ERROR - error from the Quarkdb server - FATAL
  //! 2. nullptr response - this should be retried as the client might be able
  //!      to reconnect to another machine in the cluster. If unsuccessful after
  //!      a number of retries then throw a std::runtime_error.
  //! 3. std::runtime_error - client could not send a requests and there are no
  //!      more machines available in the cluster - FATAL
  //!
  //! @param cmd command to be executed
  //!
  //! @return response object
  //----------------------------------------------------------------------------
  redisReplyPtr
  HandleResponse(std::vector<std::string> cmd);

  //----------------------------------------------------------------------------
  //! Handle response - convenience function taking as argument a pair
  //! containing the future object and the command.
  //----------------------------------------------------------------------------
  redisReplyPtr
  HandleResponse(std::future<redisReplyPtr>&& async_resp);

private:
  // The cluster members, as given in the constructor.
  size_t nextMember = 0;
  Members members;

  // the endpoint we're actually connecting to
  Endpoint targetEndpoint;

  // the endpoint given in a redirect
  Endpoint redirectedEndpoint;

  bool redirectionActive = false;

  bool transparentRedirects, exceptionsEnabled;
  bool available;

  // Network stream
  TlsConfig tlsconfig;
  NetworkStream *networkStream = nullptr;

  std::atomic<int64_t> shutdown {false};

  void startEventLoop();
  void eventLoop();
  void connect();
  void blockUntilWritable();

  std::atomic<int> sock;
  redisReader* reader = nullptr;

  void cleanup();
  bool feed(const char* buf, size_t len);
  void connectTCP();

  std::recursive_mutex mtx;
  std::queue<std::promise<redisReplyPtr>> promises;
  EventFD shutdownEventFD;

  void discoverIntercept();
  void processRedirection();
  std::unique_ptr<Handshake> handshake;
  bool handshakePending = true;
  std::thread eventLoopThread;

  //----------------------------------------------------------------------------
  // We consult this map each time a new connection is to be opened
  //----------------------------------------------------------------------------
  static std::mutex interceptsMutex;
  static std::map<std::pair<std::string, int>, std::pair<std::string, int>>
      intercepts;
};


  //----------------------------------------------------------------------------
  // Execute a command given by the begin and end iterator to a container of
  // strings.
  //----------------------------------------------------------------------------
  template <typename Iterator>
  std::future<redisReplyPtr>
  QClient::execute(const Iterator& begin, const Iterator& end)
  {
    std::uint64_t size = std::distance(begin, end);
    std::uint64_t indx = 0;
    const char* cstr[size];
    size_t sizes[size];

    for (auto it = begin; it != end; ++it) {
      cstr[indx] = it->data();
      sizes[indx] = it->size();
      ++indx;
    }

    return execute(size, cstr, sizes);
  }
}
#endif
