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
#include <hiredis/hiredis.h>
#include <unistd.h>
#include <iostream>
#include <string.h>

namespace qclient
{
  using redisReplyPtr = std::shared_ptr<redisReply>;
  //! Response type holding the future object and a vector representing
  //! the executed command.
  using AsyncResponseType = std::pair<std::future<redisReplyPtr>,
                                      std::vector<std::string>>;

class EventFD
{
public:
  EventFD()
  {
    fd = eventfd(0, EFD_NONBLOCK);
  }

  ~EventFD()
  {
    close();
  }

  void close()
  {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }

  void notify(int64_t val = 1)
  {
    int rc = write(fd, &val, sizeof(val));

    if (rc != sizeof(val)) {
      std::cerr << "qclient: CRITICAL: could not write to eventFD, return code "
                << rc << ": " << strerror(errno) << std::endl;
    }
  }

  inline int getFD() const
  {
    return fd;
  }

private:
  int fd = -1;
};

//------------------------------------------------------------------------------
//! Class QClient
//------------------------------------------------------------------------------
class QClient
{
public:
  QClient(const std::string& host, const int port, bool redirects = false,
          bool exceptions = false, std::vector<std::string> handshake = {});

  ~QClient();

  // Disallow copy and assign
  QClient(const QClient&) = delete;
  void operator=(const QClient&) = delete;

  std::future<redisReplyPtr> execute(const char* buffer, size_t len);
  std::future<redisReplyPtr> execute(const std::vector<std::string>& req);
  std::future<redisReplyPtr> execute(size_t nchunks, const char** chunks,
                                     const size_t* sizes);

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
  AsyncResponseType
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
  HandleResponse(AsyncResponseType async_resp);

private:
  // the host:port pair given in the constructor
  std::string host;
  int port;

  // the host:port pair we're actually connecting to
  std::string targetHost;
  int targetPort;

  // the host:port pair given in a redirect
  std::string redirectedHost;
  int redirectedPort;
  bool redirectionActive = false;

  bool transparentRedirects, exceptionsEnabled;
  bool available;

  std::atomic<int64_t> shutdown {false};

  void startEventLoop();
  void eventLoop();
  void connect();

  std::atomic<int> sock { -1};
  redisReader* reader = nullptr;

  void cleanup();
  bool feed(const char* buf, size_t len);
  void connectTCP();

  std::recursive_mutex mtx;
  std::queue<std::promise<redisReplyPtr>> promises;
  EventFD shutdownEventFD;

  void discoverIntercept();
  void processRedirection();
  std::vector<std::string> handshakeCommand;
  std::thread eventLoopThread;

  //----------------------------------------------------------------------------
  // We consult this map each time a new connection is to be opened
  //----------------------------------------------------------------------------
  static std::mutex interceptsMutex;
  static std::map<std::pair<std::string, int>, std::pair<std::string, int>>
      intercepts;
};

}

#endif
