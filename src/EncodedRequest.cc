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
#include "fmt/format.h"
#include <iostream>

namespace qclient {

void EncodedRequest::initFromChunks(size_t nchunks, const char** chunks, const size_t* sizes) {
  // First, format all integers we're going to need.. fmt::format_int
  // keeps its buffers on the stack.

  fmt::format_int nchunksFormatted(nchunks);

  // Abuse a stack memory region to store fmt::format_int's. I found no better
  // way to do this, while making sure all variables are kept on the stack.
  // We use placement new to construct the objects directly in a custom memory
  // region.
  char memoryRegion[sizeof(fmt::format_int) * nchunks];
  for(size_t i = 0; i < nchunks; i++) {
    new (memoryRegion + (i*sizeof(fmt::format_int))) fmt::format_int(sizes[i]);
  }

  // Calculate the required size of our buffer.
  length = 0;
  for(size_t i = 0; i < nchunks; i++) {
    length += sizes[i] + reinterpret_cast<fmt::format_int*>(&memoryRegion[sizeof(fmt::format_int)*i])->size();
    length += 1 + 2 + 2;
  }

  length += nchunksFormatted.size() + 3;

  char* buff = (char*) malloc(length);
  buff[0] = '*';
  memcpy(buff+1, nchunksFormatted.data(), nchunksFormatted.size());

  size_t pos = 1 + nchunksFormatted.size();
  buff[pos++] = '\r';
  buff[pos++] = '\n';

  for(size_t i = 0; i < nchunks; i++) {
    buff[pos++] = '$';

    fmt::format_int *formatted = reinterpret_cast<fmt::format_int*>(&memoryRegion[sizeof(fmt::format_int)*i]);
    memcpy(buff+pos, formatted->data(), formatted->size());
    pos += formatted->size();

    buff[pos++] = '\r';
    buff[pos++] = '\n';

    memcpy(buff+pos, chunks[i], sizes[i]);
    pos += sizes[i];

    buff[pos++] = '\r';
    buff[pos++] = '\n';
  }

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

EncodedRequest EncodedRequest::fuseIntoBlockAndSurround(std::deque<EncodedRequest> &&block) {
  block.emplace_front(EncodedRequest::make("MULTI"));
  block.emplace_back(EncodedRequest::make("EXEC"));
  return EncodedRequest::fuseIntoBlock(block);
}

}
