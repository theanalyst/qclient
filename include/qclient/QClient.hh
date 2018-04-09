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
#include "qclient/FutureHandler.hh"

namespace qclient
{
  class QCallback;
  class NetworkStream;
  class WriterThread;


//------------------------------------------------------------------------------
//! Describe a redisReplyPtr, in a format similar to what redis-cli would give.
//------------------------------------------------------------------------------
std::string describeRedisReply(const redisReply *const redisReply, const std::string &prefix = "");
std::string describeRedisReply(const redisReplyPtr &redisReply);

//------------------------------------------------------------------------------
//! Class handshake - inherit from here.
//! Defines the first ever request to send to the remote host, and validates
//! the response. If response is not as expected, the connection is shut down.
//------------------------------------------------------------------------------
class Handshake
{
public:
  enum class Status {
    INVALID = 0,
    VALID_INCOMPLETE,
    VALID_COMPLETE
  };

  virtual ~Handshake() {}
  virtual std::vector<std::string> provideHandshake() = 0;
  virtual Status validateResponse(const redisReplyPtr &reply) = 0;
  virtual void restart() = 0;
};

//------------------------------------------------------------------------------
//! Class RetryStrategy
//------------------------------------------------------------------------------
class RetryStrategy {
private:
  //----------------------------------------------------------------------------
  //! Private constructor, use static methods below to construct an object.
  //----------------------------------------------------------------------------
  RetryStrategy() {}

public:

  enum class Mode {
    kNoRetries = 0,
    kRetryWithTimeout,
    kInfiniteRetries
  };

  //----------------------------------------------------------------------------
  //! No retries.
  //----------------------------------------------------------------------------
  static RetryStrategy NoRetries() {
    RetryStrategy val;
    val.mode = Mode::kNoRetries;
    return val;
  }

  //----------------------------------------------------------------------------
  //! Retry, up until the specified timeout.
  //! NOTE: Timeout is per-connection, not per request.
  //----------------------------------------------------------------------------
  static RetryStrategy WithTimeout(std::chrono::seconds tm) {
    RetryStrategy val;
    val.mode = Mode::kRetryWithTimeout;
    val.timeout = tm;
    return val;
  }

  //----------------------------------------------------------------------------
  //! Infinite number of retries - hang forever if backend is not available.
  //----------------------------------------------------------------------------
  static RetryStrategy InfiniteRetries() {
    RetryStrategy val;
    val.mode = Mode::kInfiniteRetries;
    return val;
  }

  Mode getMode() const {
    return mode;
  }

  std::chrono::seconds getTimeout() const {
    return timeout;
  }

  bool active() const {
    return mode != Mode::kNoRetries;
  }

private:
  Mode mode { Mode::kNoRetries };

  //----------------------------------------------------------------------------
  //! Timeout is per-connection, not per request. Only applies if mode
  //! is kRetryWithTimeout.
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
          RetryStrategy retryStrategy = RetryStrategy::NoRetries(),
          TlsConfig tlsconfig = {},
          std::unique_ptr<Handshake> handshake = {} );

  //----------------------------------------------------------------------------
  //! Constructor taking a list of members for the cluster
  //----------------------------------------------------------------------------
  QClient(const Members &members, bool redirects = false,
          RetryStrategy retryStrategy = RetryStrategy::NoRetries(),
          TlsConfig tlsconfig = {},
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
  void execute(QCallback *callback, char* buffer, const size_t len);

  //----------------------------------------------------------------------------
  //! Convenience function to encode a redis command given as an array of char*
  //! and sizes to a redis buffer
  //!
  //! @param nchunks number of chunks in the arrays
  //! @param chunks array of char*
  //! @param sizes array of sizes of the individual chunks
  //!
  //! @return future holding a redis reply
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> execute(size_t nchunks, const char** chunks,
                                     const size_t* sizes);
  void execute(QCallback *callback, size_t nchunks, const char** chunks, const size_t* sizes);

  //----------------------------------------------------------------------------
  //! Conveninence function to encode a redis command given as a container of
  //! strings to a redis buffer
  //!
  //! @param T container: must implement begin() and end()
  //!
  //! @return future object
  //----------------------------------------------------------------------------
  template<typename T>
  std::future<redisReplyPtr> execute(const T& container);

  template<typename T>
  void execute(QCallback *callback, const T& container);

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
  void stageHandshake(const std::vector<std::string> &cont);

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

  // The future handler
  FutureHandler futureHandler;
};

  //----------------------------------------------------------------------------
  // Conveninence functions to encode a redis command given as a container of
  // strings to a redis buffer
  //----------------------------------------------------------------------------
  template <typename Container>
  std::future<redisReplyPtr>
  QClient::execute(const Container& cont)
  {
    typename Container::size_type size = cont.size();
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

  template <typename Container>
  void
  QClient::execute(QCallback *callback, const Container& cont)
  {
    typename Container::size_type size = cont.size();
    std::uint64_t indx = 0;
    const char* cstr[size];
    size_t sizes[size];

    for (auto it = cont.begin(); it != cont.end(); ++it) {
      cstr[indx] = it->data();
      sizes[indx] = it->size();
      ++indx;
    }

    execute(callback, size, cstr, sizes);
  }
}

// Instantiate std::future<redisReplyPtr> to save compile time.
extern template class std::future<qclient::redisReplyPtr>;

#endif
