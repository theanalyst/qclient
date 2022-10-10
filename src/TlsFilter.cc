// ----------------------------------------------------------------------
// File: TlsFilter.cc
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

#include "qclient/TlsFilter.hh"
#include <iostream>
#include <sstream>

#ifdef __APPLE__
  #define TLS_FILTER_ACTIVE 0
#else
  #define TLS_FILTER_ACTIVE 1
#endif

#if TLS_FILTER_ACTIVE
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif

using namespace qclient;
#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

#if TLS_FILTER_ACTIVE

static void initOpenSSL() {
  SSL_library_init();
  SSL_load_error_strings();
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
  // ERR_load_*(), ERR_load_*(), ERR_func_error_string(), ERR_get_error_line(),
  // ERR_get_error_line_data(), ERR_get_state() are not needed in OpenSSL 3.
  // They are automatically loaded:
  // https://www.openssl.org/docs/manmaster/man7/migration_guide.html
#else
  ERR_load_BIO_strings();
#endif
  OpenSSL_add_all_algorithms();
}

std::once_flag opensslFlag;

TlsFilter::TlsFilter(const TlsConfig &config, const FilterType &type, RecvFunction rc, SendFunction sd)
: tlsconfig(config), filtertype(type), recvFunc(rc), sendFunc(sd) {

  if(config.active) {
    initialize();
  }
}

void TlsFilter::initialize() {
  std::call_once(opensslFlag, initOpenSSL);

  // Create memory BIO
  wbio = BIO_new(BIO_s_mem()); // For reading from with BIO_read
  rbio = BIO_new(BIO_s_mem()); // For writing to with BIO_write

  // Create context and SSL structs
  createContext();
  configureContext();
  ssl = SSL_new(ctx);

  // Link BIOs to SSL struct
  SSL_set_bio(ssl, wbio, rbio);

  if(filtertype == FilterType::SERVER) {
    SSL_set_accept_state(ssl);
  }
  else {
    SSL_set_connect_state(ssl);
  }

  SSL_do_handshake(ssl);
  handleTraffic();
}

void TlsFilter::createContext() {
  const SSL_METHOD *method;

  if(filtertype == FilterType::SERVER) {
    method = SSLv23_server_method();
  }
  else {
    method = SSLv23_client_method();
  }

  ctx = SSL_CTX_new(method);
  SSL_CTX_set_mode(ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  if (!ctx) {
    perror("Unable to create SSL context");
    ERR_print_errors_fp(stderr);
    exit(EXIT_FAILURE);
  }
}

void TlsFilter::configureContext() {

#if defined(SSL_CTX_set_ecdh_auto)
  SSL_CTX_set_ecdh_auto(ctx, 1);
#else
  SSL_CTX_set_tmp_ecdh(ctx,
                       EC_KEY_new_by_curve_name(NID_X9_62_prime256v1));
#endif

  /* Set the key and cert */
  if(SSL_CTX_use_certificate_file(ctx, tlsconfig.certificatePath.c_str(), SSL_FILETYPE_PEM) < 0) {
    throw std::runtime_error(SSTR("Unable to set certificate file: " << tlsconfig.certificatePath));
  }

  if(SSL_CTX_use_PrivateKey_file(ctx, tlsconfig.keyPath.c_str(), SSL_FILETYPE_PEM) < 0 ) {
    throw std::runtime_error(SSTR("Unable to set certificate key: " << tlsconfig.keyPath));
  }

  // TODO: handle case where key file is encrypted
}

LinkStatus TlsFilter::handleTraffic() {
  pushCiphertext();

  //----------------------------------------------------------------------------
  // In some cases, SSL_write will give us SSL_ERROR_WANT_READ or
  // SSL_ERROR_WANT_WRITE, and require the same function is called again.
  // When that happens, we store the contents of the send operation into
  // pendingWrites, and process them as soon as possible.
  //----------------------------------------------------------------------------
  while(!pendingWrites.empty()) {
    const std::string &contents = pendingWrites.front();
    int bytes = SSL_write(ssl, contents.c_str(), contents.size());

    if(bytes == -1) break;
    if(bytes != (int) contents.size()) {
      std::cerr << "qclient: CRITICAL - wrong size by SSL_write: " << bytes << ", expected: " << contents.size() << std::endl;
      exit(EXIT_FAILURE);
    }

    pendingWrites.pop_front();
  }

  pushCiphertext();
  return 1;
}

LinkStatus TlsFilter::pushCiphertext() {
  //----------------------------------------------------------------------------
  // In this function we check whether there's any ciphertext coming from
  // OpenSSL that we ought to push onto the socket.
  //----------------------------------------------------------------------------

  const size_t BUF_SIZE = 1024 * 8;
  char ciphertext[BUF_SIZE];

  while(BIO_ctrl_pending(rbio) > 0) {
    int cipherbytes = BIO_read(rbio, ciphertext, BUF_SIZE);
    if(cipherbytes < 0) {
      std::cerr << "BIO_read from a TLS connection not successful" << std::endl;
      return -1;
    }

    sendFunc(ciphertext, cipherbytes);
  }

  return 1;
}

LinkStatus TlsFilter::send(const char *buff, int blen) {
  if(!tlsconfig.active) {
    return sendFunc(buff, blen);
  }

  // We receive plaintext here, and give it to OpenSSL for encryption.
  std::lock_guard<std::mutex> lock(mtx);
  handleTraffic();

  if(pendingWrites.empty()) {
    int bytes;
    bytes = SSL_write(ssl, buff, blen);

    if(bytes == blen) { // all OK
      handleTraffic();
      return 1;
    }
  }

  // Must queue write request
  pendingWrites.push_back(std::string(buff, blen));
  return 1;
}

RecvStatus TlsFilter::recv(char *buff, int blen, int timeout) {
  if(!tlsconfig.active) return recvFunc(buff, blen, timeout);
  std::lock_guard<std::mutex> lock(mtx);

  handleTraffic();

  // We receive ciphertext from the socket.
  const size_t BUF_SIZE = 1024 * 8;
  char ciphertext[BUF_SIZE];

  RecvStatus status = recvFunc(ciphertext, BUF_SIZE, 0);
  if(!status.connectionAlive) return status;

  // We give the ciphertext to OpenSSL for decryption.
  if(status.bytesRead > 0) {
    int bytes = BIO_write(wbio, ciphertext, status.bytesRead);

    if(bytes != status.bytesRead) {
      std::cerr << "qclient: 'should never happen' error when calling BIO_write (" << bytes << ")" << std::endl;
      return RecvStatus(false, status.bytesRead, 0);
    }
  }

  // We receive the decrypted plaintext from OpenSSL.
  ERR_clear_error();
  int plaintextBytes = SSL_read(ssl, buff, blen);

  RecvStatus ret;
  if(plaintextBytes > 0) {
    // Successful read
    ret = RecvStatus(true, 0, plaintextBytes);
  }
  else {
    int err = SSL_get_error(ssl, plaintextBytes);
    if(err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
      // not an error, connection is fine
      ret = RecvStatus(true, 0, 0);
    }
    else {
      // Ok, something's not right. Propagate to caller
      ret = RecvStatus(false, err, 0);
    }
  }

  handleTraffic();
  return ret;
}

TlsFilter::~TlsFilter() {
  close(0);

  if(ssl) {
    SSL_free(ssl);
    ssl = nullptr;
  }

  if(ctx) {
    SSL_CTX_free(ctx);
    ctx = nullptr;
  }
}

LinkStatus TlsFilter::close(int defer) {
  std::lock_guard<std::mutex> lock(mtx);

  if(ssl) {
    SSL_shutdown(ssl);
    handleTraffic();
  }
  return 0;
}

#else

TlsFilter::TlsFilter(const TlsConfig &config, const FilterType &filtertype, RecvFunction rc, SendFunction sd) {}
TlsFilter::~TlsFilter() {}

LinkStatus TlsFilter::send(const char *buff, int blen) {
  return sendFunc(buff, blen);
}

RecvStatus TlsFilter::recv(char *buff, int blen, int timeout) {
  return recvFunc(buff, blen, timeout);
}

LinkStatus TlsFilter::close(int defer) {
  return 0;
}

#endif
