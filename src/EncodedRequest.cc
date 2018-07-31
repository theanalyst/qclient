//------------------------------------------------------------------------------
// File: EncodedRequest.cc
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

#include "qclient/EncodedRequest.hh"
#include <hiredis/hiredis.h>
#include <string.h>

namespace qclient {

void EncodedRequest::initFromChunks(size_t nchunks, const char** chunks, const size_t* sizes) {
  char* buff = nullptr;
  length = redisFormatCommandArgv(&buff, nchunks, chunks, sizes);
  buffer.reset(buff);
}

EncodedRequest::EncodedRequest(size_t nchunks, const char** chunks, const size_t* sizes) {
  initFromChunks(nchunks, chunks, sizes);
}

EncodedRequest EncodedRequest::fuseIntoBlock(const std::deque<EncodedRequest> &block) {
  size_t fusedSize = 0u;
  for(size_t i = 0; i < block.size(); i++) {
    fusedSize += block[i].getLen();
  }

  char* buff = (char*) malloc(fusedSize);

  size_t pos = 0;
  for(size_t i = 0; i < block.size(); i++) {
    size_t localSize = block[i].getLen();
    memcpy(buff+pos, block[i].getBuffer(), localSize);
    pos += localSize;
  }

  return EncodedRequest(buff, fusedSize);
}

}
