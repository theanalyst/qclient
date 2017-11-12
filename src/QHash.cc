/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
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

//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Hash map implementation using QClient
//------------------------------------------------------------------------------

#include "qclient/QHash.hh"

QCLIENT_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Copy assignment
//------------------------------------------------------------------------------
QHash&
QHash::operator=(const QHash& other)
{
  mClient = other.mClient;
  mKey = other.mKey;
  return *this;
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
QHash::QHash(const QHash& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// HGET command - synchronous
//------------------------------------------------------------------------------
std::string
QHash::hget(const std::string& field)
{
  std::string resp{""};
  redisReplyPtr reply = mClient->HandleResponse({"HGET", mKey, field});

  if ((reply->type != REDIS_REPLY_STRING) && (reply->type != REDIS_REPLY_NIL)) {
    throw std::runtime_error("[FATAL] Error hget key: " + mKey + " field: "
                             + field + ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  if (reply->type == REDIS_REPLY_STRING) {
    resp.append(reply->str, reply->len);
  }

  return resp;
}

//------------------------------------------------------------------------------
// HDEL command  - synchronous
//------------------------------------------------------------------------------
bool
QHash::hdel(const std::string& field)
{
  redisReplyPtr reply = mClient->HandleResponse({"HDEL", mKey, field});

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hdel key: " + mKey + " field: "
                             + field + ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HDEL command - asynchronous
//------------------------------------------------------------------------------
std::future<redisReplyPtr>
QHash::hdel_async(const std::string& field)
{
  std::vector<std::string> cmd {"HDEL", mKey, field};
  return mClient->execute(cmd);
}

//------------------------------------------------------------------------------
// HGETALL command - synchronous
//------------------------------------------------------------------------------
std::vector<std::string>
QHash::hgetall()
{
  redisReplyPtr reply = mClient->HandleResponse({"HGETALL", mKey});

  if (reply->type != REDIS_REPLY_ARRAY) {
    throw std::runtime_error("[FATAL] Error hgetall key: " + mKey +
                             ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  std::vector<std::string> resp;
  resp.reserve(reply->elements);

  for (size_t i = 0; i < reply->elements; ++i) {
    resp.emplace_back(reply->element[i]->str, reply->element[i]->len);
  }

  return resp;
}

//------------------------------------------------------------------------------
// HEXISTS command - synchronous
//------------------------------------------------------------------------------
bool
QHash::hexists(const std::string& field)
{
  redisReplyPtr reply = mClient->HandleResponse({"HEXISTS", mKey, field});

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hexists key: " + mKey + " field: "
                             + field + ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HLEN command - synchronous
//------------------------------------------------------------------------------
long long int
QHash::hlen()
{
  redisReplyPtr reply = mClient->HandleResponse({"HLEN", mKey});

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hlen key: " + mKey +
                             ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// HLEN command - asynchronous
//------------------------------------------------------------------------------
std::future<redisReplyPtr>
QHash::hlen_async()
{
  std::vector<std::string> cmd {"HLEN", mKey};
  return mClient->execute(cmd);
}

//------------------------------------------------------------------------------
// HKEYS command - synchronous
//------------------------------------------------------------------------------
std::vector<std::string>
QHash::hkeys()
{
  redisReplyPtr reply = mClient->HandleResponse({"HKEYS", mKey});

  if (reply->type != REDIS_REPLY_ARRAY) {
    throw std::runtime_error("[FATAL] Error hkeys key: " + mKey +
                             ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  std::vector<std::string> resp;
  resp.reserve(reply->elements);

  for (size_t i = 0; i < reply->elements; ++i) {
    resp.emplace_back(reply->element[i]->str, reply->element[i]->len);
  }

  return resp;
}

//------------------------------------------------------------------------------
// HVALS command - synchronous
//------------------------------------------------------------------------------
std::vector<std::string>
QHash::hvals()
{
  redisReplyPtr reply = mClient->HandleResponse({"HVALS", mKey});

  if (reply->type != REDIS_REPLY_ARRAY) {
    throw std::runtime_error("[FATAL] Error hvals key: " + mKey +
                             ": Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  std::vector<std::string> resp;
  resp.reserve(reply->elements);

  for (size_t i = 0; i < reply->elements; ++i) {
    resp.emplace_back(reply->element[i]->str, reply->element[i]->len);
  }

  return resp;
}

//------------------------------------------------------------------------------
// HSCAN command - synchronous
//------------------------------------------------------------------------------
std::pair<std::string, std::unordered_map<std::string, std::string> >
QHash::hscan(const std::string& cursor, long long count)
{
  redisReplyPtr reply = mClient->HandleResponse({"HSCAN", mKey, cursor,
        "COUNT", std::to_string(count)});

  // Parse the Redis reply
  std::string new_cursor = std::string(reply->element[0]->str,
                                       reply->element[0]->len);
  // First element is the new cursor
  std::pair<std::string, std::unordered_map<std::string, std::string> > retc_pair;
  retc_pair.first = new_cursor;
  // Get array part of the response
  redisReply* array = reply->element[1];

  for (unsigned long i = 0; i < array->elements; i += 2) {
    retc_pair.second.emplace(
      std::string(array->element[i]->str,
                  static_cast<unsigned int>(array->element[i]->len)),
      std::string(array->element[i + 1]->str,
                  static_cast<unsigned int>(array->element[i + 1]->len)));
  }

  return retc_pair;
}

//------------------------------------------------------------------------------
// HASH multi set command - synchronous
//------------------------------------------------------------------------------
bool
QHash::hmset(std::list<std::string> lst_elem)
{
  (void) lst_elem.push_front(mKey);
  (void) lst_elem.push_front("HMSET");
  redisReplyPtr reply =
    mClient->HandleResponse(mClient->execute(lst_elem.begin(),
                                             lst_elem.end()));

  if (reply->type != REDIS_REPLY_STATUS) {
    throw std::runtime_error("[FATAL] Error hmset key: " + mKey +
                             " with multiple members: Unexpected reply type: " +
                             std::to_string(reply->type));
  }

  return true;
}

QCLIENT_NAMESPACE_END
