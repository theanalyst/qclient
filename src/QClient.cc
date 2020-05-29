// ----------------------------------------------------------------------
// File: QClient.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#include "qclient/QClient.hh"
#include "qclient/Utils.hh"
#include "qclient/network/HostResolver.hh"
#include "qclient/network/AsyncConnector.hh"
#include "network/NetworkStream.hh"
#include <unistd.h>
#include <string.h>
#include <poll.h>
#include <signal.h>
#include <fcntl.h>
#include <sstream>
#include <iterator>
#include "qclient/Logger.hh"
#include "WriterThread.hh"
#include "EndpointDecider.hh"
#include "ConnectionCore.hh"
#include "qclient/GlobalInterceptor.hh"

//------------------------------------------------------------------------------
//! Instantiate a few templates inside this compilation unit, to save compile
//! time. The alternative is to have every single compilation unit which
//! includes QClient.hh instantiate them, which increases compilation time.
//------------------------------------------------------------------------------
template class std::future<qclient::redisReplyPtr>;
#if HAVE_FOLLY == 1
template class folly::Future<qclient::redisReplyPtr>;
#endif

using namespace qclient;
#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl;

//------------------------------------------------------------------------------
// Constructor taking host and port
//-----------------------------------------------------------------------------
QClient::QClient(const std::string& host_, const int port_, Options &&opts)
  : members(host_, port_), options(std::move(opts)), faultInjector(*this)
{
  startEventLoop();
}


//------------------------------------------------------------------------------
// Constructor taking list of members for the cluster
//------------------------------------------------------------------------------
QClient::QClient(const Members& members_, Options &&opts)
  : members(members_), options(std::move(opts)), faultInjector(*this)
{
  startEventLoop();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QClient::~QClient()
{
  shutdownEventFD.notify();
  eventLoopThread.join();
  cleanup(true);
}

//------------------------------------------------------------------------------
// Primary execute command that takes a redis encoded buffer and sends it
// over the network
//------------------------------------------------------------------------------
void QClient::execute(QCallback *callback, EncodedRequest &&req) {
  connectionCore->stage(callback, std::move(req));
}

std::future<redisReplyPtr> QClient::execute(EncodedRequest &&req) {
  return connectionCore->stage(std::move(req));
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> QClient::follyExecute(EncodedRequest &&req) {
  return connectionCore->follyStage(std::move(req));
}
#endif

//------------------------------------------------------------------------------
// Execute a MULTI block.
//------------------------------------------------------------------------------
void QClient::execute(QCallback *callback, std::deque<EncodedRequest> &&reqs) {
  size_t ignoredResponses = reqs.size() + 1;

  connectionCore->stage(
    callback,
    EncodedRequest::fuseIntoBlockAndSurround(std::move(reqs)),
    ignoredResponses
  );
}

std::future<redisReplyPtr> QClient::execute(std::deque<EncodedRequest> &&reqs) {
  size_t ignoredResponses = reqs.size() + 1;

  return connectionCore->stage(
    EncodedRequest::fuseIntoBlockAndSurround(std::move(reqs)),
    ignoredResponses
  );
}

#if HAVE_FOLLY == 1
folly::Future<redisReplyPtr> QClient::follyExecute(std::deque<EncodedRequest> &&req) {
  size_t ignoredResponses = req.size() + 1;

  return connectionCore->follyStage(
    EncodedRequest::fuseIntoBlockAndSurround(std::move(req)),
    ignoredResponses
  );
}
#endif

//------------------------------------------------------------------------------
// Event loop for the client
//------------------------------------------------------------------------------
void QClient::startEventLoop()
{
  // Initialize a simple logger if user has not provided one
  if(!options.logger) {
    options.logger = std::make_shared<StandardErrorLogger>();
  }

  // If no handshake is present, and user is asking to prime
  // connection: Set handshake to a simple Pinger.
  if(!options.handshake && options.ensureConnectionIsPrimed) {
    options.handshake.reset(new PingHandshake());
  }

  hostResolver = std::make_unique<HostResolver>(options.logger.get());
  endpointDecider = std::make_unique<EndpointDecider>(options.logger.get(), hostResolver.get(), members);

  // Give some leeway when starting up before declaring the cluster broken.
  lastAvailable = std::chrono::steady_clock::now();

  connectionCore.reset(new ConnectionCore(options.logger.get(),
    options.handshake.get(), options.backpressureStrategy, options.transparentRedirects, options.messageListener.get(), options.exclusivePubsub));
  writerThread.reset(new WriterThread(options.logger.get(), *connectionCore.get(), shutdownEventFD));
  eventLoopThread.reset(&QClient::eventLoop, this);
}

//------------------------------------------------------------------------------
// Feed bytes from the socket into the response builder
//------------------------------------------------------------------------------
bool QClient::feed(const char* buf, size_t len)
{
  responseBuilder.feed(buf, len);

  while (true) {
    redisReplyPtr rr;
    ResponseBuilder::Status status = responseBuilder.pull(rr);

    if(status == ResponseBuilder::Status::kProtocolError) {
      return false;
    }

    if(status == ResponseBuilder::Status::kIncomplete) {
      // We need more bytes before a full response can be built, go back
      // to the event loop to pull more bytes.
      return true;
    }

    // --- We have a new response from the server!

    // Is this a redirect?
    if (options.transparentRedirects && rr->type == REDIS_REPLY_ERROR &&
        strncmp(rr->str, "MOVED ", strlen("MOVED ")) == 0) {

      std::vector<std::string> response = split(std::string(rr->str, rr->len), " ");
      RedisServer redirect;

      if (response.size() == 3 && parseServer(response[2], redirect)) {
        endpointDecider->registerRedirection(Endpoint(redirect.host, redirect.port));
        return false;
      }
    }

    // "Normal" response, let the connection handler take care of it.
    if(!connectionCore->consumeResponse(std::move(rr))) {
      // An error has been signalled, this connection cannot go on.
      return false;
    }

    // We're all good, satisfy request.
    successfulResponses = true;
  }

  return true;
}

//------------------------------------------------------------------------------
// Should we purge any requests currently queued inside WriterThread?
//------------------------------------------------------------------------------
bool QClient::shouldPurgePendingRequests() {
  if(options.retryStrategy.getMode() == RetryStrategy::Mode::kInfiniteRetries) {
    //--------------------------------------------------------------------------
    // Infinite retries, nope.
    //--------------------------------------------------------------------------
    return false;
  }

  if(options.retryStrategy.getMode() == RetryStrategy::Mode::kRetryWithTimeout &&
     lastAvailable + options.retryStrategy.getTimeout() >= std::chrono::steady_clock::now()) {
    //--------------------------------------------------------------------------
    // Timeout has not expired yet, nope.
    //--------------------------------------------------------------------------
    return false;
  }

  //----------------------------------------------------------------------------
  // We shouldn't purge while trying to connect.. Otherwise, the following
  // will occur:
  //
  // qclient::QClient qcl( ... );
  // qcl.execute("PING") -> failed because the first ServiceEndpoint was
  //                        invalid, even though the rest were good
  //
  // Instead, we should consider purging only after we've tried all service
  // endpoints.
  //----------------------------------------------------------------------------
  if(!successfulResponsesEver && !endpointDecider->madeFullCircle()) {
    //--------------------------------------------------------------------------
    // We're still trying out endpoints, no purge
    //--------------------------------------------------------------------------
    return false;
  }

  //----------------------------------------------------------------------------
  // Yes, purge.
  //----------------------------------------------------------------------------
  return true;
}

//------------------------------------------------------------------------------
// Cleanup before reconnection or when exiting
//------------------------------------------------------------------------------
void QClient::cleanup(bool shutdown)
{
  writerThread->deactivate();
  networkStream.reset();

  responseBuilder.restart();
  successfulResponsesEver = successfulResponsesEver | successfulResponses;
  successfulResponses = false;

  if(shouldPurgePendingRequests()) {

    size_t previouslyPending = connectionCore->clearAllPending();
    if(shutdown) {
      QCLIENT_LOG(options.logger, LogLevel::kDebug, SSTR("Shutting down QClient, discarding " << previouslyPending << " pending requests"));
    }
    else {
      QCLIENT_LOG(options.logger, LogLevel::kInfo, SSTR("Backend is unavailable, discarding " << previouslyPending << " pending requests"));
    }

  }

  connectionCore->reconnection();
}

//------------------------------------------------------------------------------
// Set up TCP connection
//------------------------------------------------------------------------------
void QClient::connectTCP()
{
  // TODO(gbitzes): Fix fault injection eventually..
  // if(faultInjector.hasPartition(untranslatedTargetEndpoint)) {
  //   networkStream.reset();
  //   return;
  // }

  ServiceEndpoint endpoint;

  if(!endpointDecider->getNextEndpoint(endpoint)) {
    return;
  }

  AsyncConnector connector(endpoint);
  if(!connector.blockUntilReady(shutdownEventFD.getFD(), options.tcpTimeout)) {
    return;
  }

  if(!connector.ok()) {
    QCLIENT_LOG(options.logger, LogLevel::kInfo, "Encountered an error when connecting to " << endpoint.getString() << ": " << connector.getError());
    return;
  }

  networkStream.reset(new NetworkStream(connector.release(), options.tlsconfig));
  if(!networkStream->ok()) {
    return;
  }

  notifyConnectionEstablished();
  writerThread->activate(networkStream.get());
}

//------------------------------------------------------------------------------
// Connect
//------------------------------------------------------------------------------
void QClient::connect()
{
  currentConnectionEpoch++;
  if(currentConnectionEpoch != 1) {
    cleanup(false);
  }
  connectTCP();
}

//------------------------------------------------------------------------------
// Main event loop thread, handles processing of incoming requests and
// reconnects in case of network instabilities.
//------------------------------------------------------------------------------
void QClient::eventLoop(ThreadAssistant &assistant)
{
  signal(SIGPIPE, SIG_IGN);
  std::chrono::milliseconds backoff(1);

  while (true) {
    this->connect();

    bool receivedBytes = handleConnectionEpoch(assistant);
    if(receivedBytes) {
      backoff = std::chrono::milliseconds(1);
    }

    assistant.wait_for(backoff);

    if (assistant.terminationRequested()) {
      feed(NULL, 0);
      break;
    }

    // Give some more leeway, update lastAvailable after sleeping.
    if(successfulResponses) {
      lastAvailable = std::chrono::steady_clock::now();
    }

    if (backoff < std::chrono::milliseconds(2048)) {
      backoff++;
    }
  }
}

//------------------------------------------------------------------------------
// Return fault injector object for this QClient
//------------------------------------------------------------------------------
FaultInjector& QClient::getFaultInjector() {
  return faultInjector;
}

//------------------------------------------------------------------------------
// Notification from FaultInjector that fault injections were updated
//------------------------------------------------------------------------------
void QClient::notifyFaultInjectionsUpdated() {
  // shutdownEventFD.notify();
}

//------------------------------------------------------------------------------
// Attach reconnection listener. The underlying object must remain alive
// as long as the reconnection listener is attached!
//------------------------------------------------------------------------------
void QClient::attachListener(ReconnectionListener *listener) {
  std::unique_lock<std::mutex> lock(reconnectionListenersMtx);
  reconnectionListeners.insert(listener);
}

//------------------------------------------------------------------------------
// Detach reconnection listener. It's now safe to delete the underlying
// object.
//
// Returns true if the given object was found to be registered and was
// removed, false otherwise.
//------------------------------------------------------------------------------
bool QClient::detachListener(ReconnectionListener *listener) {
  std::unique_lock<std::mutex> lock(reconnectionListenersMtx);

  auto it = reconnectionListeners.find(listener);
  if(it == reconnectionListeners.end()) {
    return false;
  }

  reconnectionListeners.erase(it);
  return true;
}

//------------------------------------------------------------------------------
// Notify that a connection has been established
//------------------------------------------------------------------------------
void QClient::notifyConnectionEstablished() {
  std::unique_lock<std::mutex> lock(reconnectionListenersMtx);

  for(auto it = reconnectionListeners.begin(); it != reconnectionListeners.end(); it++) {
    (*it)->notifyConnectionEstablished(currentConnectionEpoch);
  }
}

//------------------------------------------------------------------------------
// Notify that a connection has been lost
//------------------------------------------------------------------------------
void QClient::notifyConnectionLost(int errc, const std::string &err) {
  std::unique_lock<std::mutex> lock(reconnectionListenersMtx);

  for(auto it = reconnectionListeners.begin(); it != reconnectionListeners.end(); it++) {
    (*it)->notifyConnectionLost(currentConnectionEpoch, errc, err);
  }
}

//------------------------------------------------------------------------------
// Handles a single "connection epoch". If the current socket breaks, we return
// and let the parent handle the error.
//
// Returns whether, during this connection epoch, any bytes at all were received
// from the server.
//------------------------------------------------------------------------------
bool QClient::handleConnectionEpoch(ThreadAssistant &assistant) {
  const size_t BUFFER_SIZE = 1024 * 2;
  char buffer[BUFFER_SIZE];
  bool receivedBytes = false;

  if(!networkStream || !networkStream->ok()) {
    return false;
  }

  struct pollfd polls[2];
  polls[0].fd = shutdownEventFD.getFD();
  polls[0].events = POLLIN;
  polls[1].fd = networkStream->getFd();
  polls[1].events = POLLIN;

  RecvStatus status(true, 0, 0);
  while (networkStream->ok()) {
    // If the previous iteration returned any bytes at all, try to read again
    // without polling. It could be that there's more data cached inside
    // OpenSSL, which poll() will not detect.

    if(status.bytesRead <= 0) {
      int rpoll = poll(polls, 2, 60);
      if(rpoll < 0 && errno != EINTR) {
        // something's wrong, try to reconnect
        break;
      }
    }

    if( (polls[0].revents != 0) || assistant.terminationRequested()) {
      notifyConnectionLost(0, "shutdown requested");
      break;
    }

    // looks like a legit connection
    status = networkStream->recv(buffer, BUFFER_SIZE, 0);

    if(!status.connectionAlive) {
      break; // connection died on us
    }

    if(status.bytesRead > 0 && !feed(buffer, status.bytesRead)) {
      notifyConnectionLost(EINVAL, "protocol violation");
      break; // protocol violation
    }
    else {
      receivedBytes = true;
    }
  }

  if(!networkStream->ok()) {
    notifyConnectionLost(networkStream->getErrno(), networkStream->getError());
  }

  return receivedBytes;
}

//------------------------------------------------------------------------------
// Wrapper function for exists command
//------------------------------------------------------------------------------
long long int
QClient::exists(const std::string& key)
{
  redisReplyPtr reply = exec("EXISTS", key).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error exists key: " + key +
                             ": Unexpected/null reply ");
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Wrapper function for del command
//------------------------------------------------------------------------------
long long int
QClient::del(const std::string& key)
{
  redisReplyPtr reply = exec("DEL", key).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error del key: " + key +
                             ": Unexpected/null reply ");
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Check whether we're currently connected by sending a PING -- synchronous
// operation with the given timeout.
//------------------------------------------------------------------------------
Status QClient::checkConnection(std::chrono::milliseconds timeout) {
  std::future<qclient::redisReplyPtr> fut = this->exec("PING");

  if(fut.wait_for(timeout) != std::future_status::ready) {
    return Status(ETIME, "time-out while waiting on PING reply");
  }

  qclient::redisReplyPtr reply = fut.get();

  if (!reply) {
    return Status(ENOTCONN, "connection not active");
  }

  if (reply->type != REDIS_REPLY_STATUS || std::string(reply->str, reply->len) != "PONG") {
    return Status(EINVAL, SSTR("Received unexpected response to PING request: " << qclient::describeRedisReply(reply)));
  }

  return Status();
}

