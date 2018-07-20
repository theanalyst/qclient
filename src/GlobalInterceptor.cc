//------------------------------------------------------------------------------
// File: GlobalInterceptor.cc
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

#include "qclient/GlobalInterceptor.hh"
#include <mutex>
#include <map>

namespace {
std::mutex interceptsMutex;
std::map<qclient::Endpoint, qclient::Endpoint> intercepts;
}

namespace qclient {

//------------------------------------------------------------------------------
// The intercepts machinery
//------------------------------------------------------------------------------
void GlobalInterceptor::addIntercept(const Endpoint &from, const Endpoint &to)
{
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts[from] = to;
}

void GlobalInterceptor::clearIntercepts()
{
  std::lock_guard<std::mutex> lock(interceptsMutex);
  intercepts.clear();
}

Endpoint GlobalInterceptor::translate(const Endpoint &target)
{
  std::lock_guard<std::mutex> lock(interceptsMutex);
  auto it = intercepts.find(target);

  if(it != intercepts.end()) {
    return it->second;
  }

  return target;
}

}
