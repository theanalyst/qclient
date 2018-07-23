//------------------------------------------------------------------------------
// File: Buffer.hh
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

#ifndef QCLIENT_ENCODED_REQUEST_HH
#define QCLIENT_ENCODED_REQUEST_HH

#include <memory>

namespace qclient {

//------------------------------------------------------------------------------
// A class to represent an encoded redis request. Move-only type, it is not
// possible to copy it, as there's no need to. This prevents accidental
// inefficiencies.
//------------------------------------------------------------------------------
class EncodedRequest {
public:
  EncodedRequest(char* buff, size_t len) {
    buffer.reset(buff);
    length = len;
  }

  EncodedRequest(size_t nchunks, const char** chunks, const size_t* sizes);

  template <typename Container>
  EncodedRequest(const Container& cont)
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

    initFromChunks(size, cstr, sizes);
  }

  char* getBuffer() {
    return buffer.get();
  }

  size_t getLen() const {
    return length;
  }

private:
  void initFromChunks(size_t nchunks, const char** chunks, const size_t* sizes);

  struct Deleter {
    void operator()(char* b) { free(b); }
  };

  std::unique_ptr<char, Deleter> buffer;
  size_t length;
};

}

#endif
