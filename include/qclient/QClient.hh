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
#include "qclient/Utils.hh"

namespace qclient
{
  class NetworkStream;
  class WriterThread;
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
//! Struct RetryStrategy
//------------------------------------------------------------------------------
struct RetryStrategy {
  RetryStrategy() {}
  RetryStrategy(bool en, std::chrono::seconds tim)
  : enabled(en), timeout(tim) {}

  bool enabled = false;

  //----------------------------------------------------------------------------
  //! Timeout is per-connection, not per request.
  //----------------------------------------------------------------------------
  std::chrono::seconds timeout {0};
};

//------------------------------------------------------------------------------
//! Class QClient
//------------------------------------------------------------------------------
class QClient
{
public:
  //----------------------------------------------------------------------------
  //! Constructor taking simple host and port
  //----------------------------------------------------------------------------
  QClient(const std::string &host, int port, bool redirects = false,
          RetryStrategy retryStrategy = {}, TlsConfig tlsconfig = {},
          std::unique_ptr<Handshake> handshake = {} );

  //----------------------------------------------------------------------------
  //! Constructor taking a list of members for the cluster
  //----------------------------------------------------------------------------
  QClient(const Members &members, bool redirects = false,
          RetryStrategy retryStrategy = {}, TlsConfig tlsconfig = {},
          std::unique_ptr<Handshake> handshake = {} );

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
  //! Primary execute command that takes a redis encoded buffer and sends it
  //! over the network
  //!
  //! @param buffer Redis encoded buffer containing a request
  //! @param len length of the buffer
  //!
  //! @return future holding a redis reply
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> execute(char* buffer, const size_t len);

  //----------------------------------------------------------------------------
  //! Convenience function to encode redis command given as a std::string to
  //! a redis buffer.
  //!
  //! @param cmd redis command e.g. "GET <key> <value>"
  //!
  //! @return future holding a redis reply
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> execute(const std::string& cmd);

  //----------------------------------------------------------------------------
  //! Convenience function to encode a redis command given as an array of char*
  //! and sizes to a redis buffer.
  //!
  //! @param nchunks number of chunks in the arrays
  //! @param chunks array of char*
  //! @param sizes array of sizes of the individual chunks
  //!
  //! @return future holding a redis reply
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> execute(size_t nchunks, const char** chunks,
                                     const size_t* sizes);

  //----------------------------------------------------------------------------
  //! Conveninence function to encode a redis command given as a container of
  //! strings to a redis buffer.
  //!
  //! @param T container: must implement begin() and end()
  //!
  //! @return future object
  //----------------------------------------------------------------------------
  template<typename T>
  std::future<redisReplyPtr> execute(const T& container);

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
  //!
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
  HandleResponse(std::future<redisReplyPtr>&& async_resp,
                 const std::string& cmd);

  //----------------------------------------------------------------------------
  //! Convenience handle response method taking as argument a container holding
  //! the command to be executed
  //!
  //! @param cmd container of strings representing the command
  //!
  //! @return response object
  //----------------------------------------------------------------------------
  template <typename Container>
  redisReplyPtr
  HandleResponse(const Container& cmd);

private:
  // The cluster members, as given in the constructor.
  size_t nextMember = 0;
  Members members;

  // the endpoint we're actually connecting to
  Endpoint targetEndpoint;

  // the endpoint given in a redirect
  Endpoint redirectedEndpoint;

  bool redirectionActive = false;

  bool transparentRedirects;
  RetryStrategy retryStrategy;

  std::chrono::steady_clock::time_point lastAvailable;
  bool successfulResponses;

  // Network stream
  TlsConfig tlsconfig;
  NetworkStream *networkStream = nullptr;

  std::atomic<int64_t> shutdown {false};

  void startEventLoop();
  void eventLoop();
  void connect();

  redisReader* reader = nullptr;

  void cleanup();
  bool feed(const char* buf, size_t len);
  void connectTCP();

  std::recursive_mutex mtx;
  WriterThread *writerThread = nullptr;
  EventFD shutdownEventFD;

  void discoverIntercept();
  void processRedirection();
  std::unique_ptr<Handshake> handshake;
  bool handshakePending = true;
  std::thread eventLoopThread;

  // We consult this map each time a new connection is to be opened
  static std::mutex interceptsMutex;
  static std::map<std::pair<std::string, int>, std::pair<std::string, int>>
      intercepts;
};

  //----------------------------------------------------------------------------
  // Conveninence function to encode a redis command given as a container of
  // strings to a redis buffer
  //----------------------------------------------------------------------------
  template <typename Container>
  std::future<redisReplyPtr>
  QClient::execute(const Container& cont)
  {
    std::uint64_t size = cont.size();
    std::uint64_t indx = 0;
    const char* cstr[size];
    size_t sizes[size];

    for (auto it = cont.begin(); it != cont.end(); ++it) {
      cstr[indx] = it->data();
      sizes[indx] = it->size();
      ++indx;
    }

    return execute(size, cstr, sizes);
  }

  //------------------------------------------------------------------------------
  // Convenience method to handle response
  //------------------------------------------------------------------------------
  template <typename Container>
  redisReplyPtr
  QClient::HandleResponse(const Container& cont)
  {
    fmt::MemoryWriter out;

    if (!cont.empty()) {
      auto it = cont.begin();
      out << *it;

      while (++it != cont.end()) {
        out << " " << *it;
      }
    }

    return HandleResponse(execute(out.str()), out.str());
  }
}
#endif
