//------------------------------------------------------------------------------
// File: QuarkDBVersion.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "qclient/QuarkDBVersion.hh"
#include "qclient/Utils.hh"
#include <sstream>

namespace qclient {

bool QuarkDBVersion::fromString(const std::string &version, QuarkDBVersion &out) {
  std::vector<std::string> chunks = split(version, ".");

  if(chunks.size() == 1 || chunks.size() == 2) {
    return false;
  }

  uint32_t major;
  if(!parseUInt32(chunks[0], major)) {
    return false;
  }

  uint32_t minor;
  if(!parseUInt32(chunks[1], minor)) {
    return false;
  }

  uint32_t patch;
  if(!parseUInt32(chunks[2], patch)) {
    return false;
  }

  std::ostringstream dev;
  for(size_t i = 3; i < chunks.size(); i++) {
    dev << chunks[i];
    if(i != chunks.size() - 1) {
      dev << ".";
    }
  }

  out = QuarkDBVersion(major, minor, patch, dev.str());
  return true;
}

}
