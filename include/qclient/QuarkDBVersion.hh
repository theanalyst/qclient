// ----------------------------------------------------------------------
// File: VersionParser.hh
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

#ifndef QCLIENT_VERSION_PARSER_HH
#define QCLIENT_VERSION_PARSER_HH

#include <stdint.h>
#include <string>
#include <sstream>

namespace qclient {

class QuarkDBVersion {
public:

  QuarkDBVersion() : mMajor(0), mMinor(0), mPatch(0), mDev() {}

  QuarkDBVersion(uint32_t major, uint32_t minor, uint32_t patch, const std::string &dev)
  : mMajor(major), mMinor(minor), mPatch(patch), mDev(dev) {}

  uint32_t getMajor() const {
    return mMajor;
  }

  uint32_t getMinor() const {
    return mMinor;
  }

  uint32_t getPatch() const {
    return mPatch;
  }

  std::string getDev() const {
    return mDev;
  }

  bool operator==(const QuarkDBVersion &other) const {
    return mMajor == other.mMajor &&
           mMinor == other.mMinor &&
           mPatch == other.mPatch &&
           mDev   == other.mDev;
  }

  bool operator!=(const QuarkDBVersion &other) const {
    return !(*this == other);
  }

  bool operator<(const QuarkDBVersion &other) const {
    if(mMajor != other.mMajor) {
      return mMajor < other.mMajor;
    }

    if(mMinor != other.mMinor) {
      return mMinor < other.mMinor;
    }

    if(mPatch != other.mPatch) {
      return mPatch < other.mPatch;
    }

    return mDev < other.mDev;
  }

  bool operator<=(const QuarkDBVersion &other) const {
    return (*this < other) || (*this == other);
  }

  bool operator>(const QuarkDBVersion &other) const {
    return !(*this <= other);
  }

  bool operator>=(const QuarkDBVersion &other) const {
    return !(*this < other);
  }

  std::string toString() const {
    std::ostringstream ss;

    if(!mDev.empty()) {
      ss << mMajor << "." << mMinor << "." << mPatch << "." << mDev;
    }
    else {
      ss << mMajor << "." << mMinor << "." << mPatch;
    }

    return ss.str();
  }

  static bool fromString(const std::string &version, QuarkDBVersion &out);

private:
  uint32_t mMajor;
  uint32_t mMinor;
  uint32_t mPatch;
  std::string mDev;
};

}

#endif
