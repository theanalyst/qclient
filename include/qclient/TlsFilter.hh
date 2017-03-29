//------------------------------------------------------------------------------
// File: TlsFilter.hh
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

#ifndef __QCLIENT_TLS_FILTER_H__
#define __QCLIENT_TLS_FILTER_H__

#include <string>
#include <functional>
#include <mutex>
#include <list>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include "qclient/Namespace.hh"

QCLIENT_NAMESPACE_BEGIN

enum class FilterType {
  CLIENT = 0,     // act as a client
  SERVER          // act as a server
};

struct RecvStatus {
  RecvStatus() {}
  RecvStatus(bool alive, int err, int bytes)
  : connectionAlive(alive), errcode(err), bytesRead(bytes) {}

  bool connectionAlive;
  int errcode; // anything other than 0 means connection is broken
  int bytesRead;
};

struct TlsConfig {
  TlsConfig() {} // disabled, filter should act as a passthrough
  TlsConfig(const std::string &certpath_, const std::string &keypath_,
            const std::string &pw_, const std::string &capath_, bool verify_)
  : active(true), certificatePath(certpath_), keyPath(keypath_), decryptionPassword(pw_), capath(capath_), verify(verify_) {}

  bool active = false; // if false, filter acts as passthrough
  std::string certificatePath; // certificate path
  std::string keyPath; // certificate key
  std::string decryptionPassword; // in case certificate key is encrypted
  std::string capath; // certificate store against which to verify peer certs
  bool verify = true; // verify peer certificate
};

using LinkStatus = int;
using RecvFunction = std::function<RecvStatus(char *buf, int len, int timeout)>;
using SendFunction = std::function<LinkStatus(const char *buf, int len)>;

class TlsFilter {
public:
  TlsFilter(const TlsConfig &config, const FilterType &filtertype, RecvFunction rc, SendFunction sd);
  ~TlsFilter();

  LinkStatus send(const char *buff, int blen);
  RecvStatus recv(char *buff, int blen, int timeout);
  LinkStatus close(int defer);
private:
  void initialize();
  void createContext();
  void configureContext();

  LinkStatus handleTraffic();
  LinkStatus pushCiphertext();

  std::mutex mtx;

  TlsConfig tlsconfig;
  FilterType filtertype;

  SSL_CTX *ctx;
  SSL *ssl;
  BIO *rbio;
  BIO *wbio;

  RecvFunction recvFunc;
  SendFunction sendFunc;

  std::list<std::string> pendingWrites;
};

QCLIENT_NAMESPACE_END

#endif
