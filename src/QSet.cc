//------------------------------------------------------------------------------
//! @file QSet.cc
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
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

#include "qclient/QSet.hh"

using namespace std;

QCLIENT_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Copy assignment
//------------------------------------------------------------------------------
QSet&
QSet::operator=(const QSet& other)
{
  mClient = other.mClient;
  mKey = other.mKey;
  return *this;
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
QSet::QSet(const QSet& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// Redis SET add command for multiple members - synchronous
//------------------------------------------------------------------------------
long long int QSet::sadd(std::list<std::string> lst_elem)
{
  (void) lst_elem.push_front(mKey);
  (void) lst_elem.push_front("SADD");
  redisReplyPtr reply = mClient->HandleResponse(lst_elem.begin(), lst_elem.end());

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error sadd key: " + mKey +
                             " with multiple members: Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Redis SET remove command for multiple members - synchronous
//------------------------------------------------------------------------------
long long int QSet::srem(std::list<std::string> lst_elem)
{
  (void) lst_elem.push_front(mKey);
  (void) lst_elem.push_front("SREM");
  redisReplyPtr reply = mClient->HandleResponse(lst_elem.begin(), lst_elem.end());

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error srem key: " + mKey +
                             " with multiple members: Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Redis SET size command - synchronous
//------------------------------------------------------------------------------
long long int QSet::scard()
{
  redisReplyPtr reply = mClient->HandleResponse({"SCARD", mKey});

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error scard key: " + mKey +
                             " : Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Redis SET smembers command - synchronous
//------------------------------------------------------------------------------
std::set<std::string> QSet::smembers()
{
  redisReplyPtr reply = mClient->HandleResponse({"SMEMBERS", mKey});

  if (reply->type != REDIS_REPLY_ARRAY) {
    throw std::runtime_error("[FATAL] Error smembers key: " + mKey +
                             " : Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  std::set<std::string> ret;

  for (size_t i = 0; i < reply->elements; ++i) {
    ret.emplace(reply->element[i]->str, reply->element[i]->len);
  }

  return ret;
}

//------------------------------------------------------------------------------
// Redis SET SCAN command - synchronous
//------------------------------------------------------------------------------
std::pair< std::string, std::vector<std::string> >
QSet::sscan(std::string cursor, long long count)
{
  // TODO (gbitzes): add support for COUNT parameter
  //  auto future = mClient->execute({"SSCAN", mKey, cursor, "COUNT",
  //  std::to_string(count)});
  redisReplyPtr reply = mClient->HandleResponse({"SSCAN", mKey, cursor});

  // Parse the Redis reply
  std::string new_cursor {reply->element[0]->str,
                          static_cast<unsigned int>(reply->element[0]->len)};
  // First element is the new cursor
  std::pair<std::string, std::vector<std::string> > retc_pair;
  retc_pair.first = new_cursor;
  // Get arrary part of the response
  redisReply* reply_ptr =  reply->element[1];

  for (unsigned long i = 0; i < reply_ptr->elements; ++i) {
    retc_pair.second.emplace_back(reply_ptr->element[i]->str,
                                  static_cast<unsigned int>(reply_ptr->element[i]->len));
  }

  return retc_pair;
}

//------------------------------------------------------------------------------
// Redis SET add command for multiple elements - asynchronous
//------------------------------------------------------------------------------
AsyncResponseType
QSet::sadd_async(std::set<std::string> set_elem)
{
  std::vector<std::string> cmd;
  (void) cmd.push_back("SADD");
  (void) cmd.push_back(mKey);
  (void) cmd.insert(cmd.end(), set_elem.begin(), set_elem.end());
  return std::make_pair(mClient->execute(cmd), std::move(cmd));
}

//------------------------------------------------------------------------------
// Redis SET add command for multiple elements - asynchronous
//------------------------------------------------------------------------------
AsyncResponseType
QSet::sadd_async(std::list<std::string> set_elem)
{
  std::vector<std::string> cmd;
  (void) cmd.push_back("SADD");
  (void) cmd.push_back(mKey);
  (void) cmd.insert(cmd.end(), set_elem.begin(), set_elem.end());
  return std::make_pair(mClient->execute(cmd), std::move(cmd));
}

QCLIENT_NAMESPACE_END
