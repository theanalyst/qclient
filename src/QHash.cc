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
  redisReplyPtr reply = mClient->exec("HGET", mKey, field).get();

  if ((reply == nullptr) || ((reply->type != REDIS_REPLY_STRING) &&
                             (reply->type != REDIS_REPLY_NIL))) {
    throw std::runtime_error("[FATAL] Error hget key: " + mKey + " field: "
                             + field + ": Unexpected/null reply");
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
  redisReplyPtr reply = mClient->exec("HDEL", mKey, field).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error hdel key: " + mKey + " field: "
                             + field + ": Unexpected/null reply");
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HDEL command - asynchronous
//------------------------------------------------------------------------------
void
QHash::hdel_async(const std::string& field, AsyncHandler* ah)
{
  ah->Register(mClient, {"HDEL", mKey, field});
}

//------------------------------------------------------------------------------
// HGETALL command - synchronous
//------------------------------------------------------------------------------
std::vector<std::string>
QHash::hgetall()
{
  redisReplyPtr reply = mClient->exec("HGETALL", mKey).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_ARRAY)) {
    throw std::runtime_error("[FATAL] Error hgetall key: " + mKey +
                             ": Unexpected/null reply");
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
  redisReplyPtr reply = mClient->exec("HEXISTS", mKey, field).get();

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hexists key: " + mKey + " field: "
                             + field + ": Unexpected/null reply");
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HLEN command - synchronous
//------------------------------------------------------------------------------
long long int
QHash::hlen()
{
  redisReplyPtr reply = mClient->exec("HLEN", mKey).get();

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hlen key: " + mKey +
                             ": Unexpected/null reply");
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// HLEN command - asynchronous
//------------------------------------------------------------------------------
void
QHash::hlen_async(AsyncHandler* ah)
{
  ah->Register(mClient, {"HLEN", mKey});
}

//------------------------------------------------------------------------------
// HKEYS command - synchronous
//------------------------------------------------------------------------------
std::vector<std::string>
QHash::hkeys()
{
  redisReplyPtr reply = mClient->exec("HKEYS", mKey).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_ARRAY)) {
    throw std::runtime_error("[FATAL] Error hkeys key: " + mKey +
                             ": Unexpected/null reply");
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
  redisReplyPtr reply = mClient->exec("HVALS", mKey).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_ARRAY)) {
    throw std::runtime_error("[FATAL] Error hvals key: " + mKey +
                             ": Unexpected/null reply");
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
std::pair<std::string, std::map<std::string, std::string> >
QHash::hscan(const std::string& cursor, long long count)
{
  redisReplyPtr reply = mClient->exec("HSCAN", mKey, cursor, "COUNT",
                                      fmt::to_string(count)).get();

  if (reply == nullptr) {
    throw std::runtime_error("[FATAL] Error hscan key: " + mKey +
                             ": Unexpected/null reply");
  }

  // Parse the Redis reply
  std::string new_cursor = std::string(reply->element[0]->str,
                                       reply->element[0]->len);
  // First element is the new cursor
  std::pair<std::string, std::map<std::string, std::string> > retc_pair;
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
bool QHash::hmset(std::list<std::string> lst_elem)
{
  (void) lst_elem.push_front(mKey);
  (void) lst_elem.push_front("HMSET");
  redisReplyPtr reply =  mClient->execute(lst_elem).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_STATUS)) {
    throw std::runtime_error("[FATAL] Error hmset key: " + mKey +
                             " with multiple members: Unexpected/null reply type");
  }

  return true;
}

//------------------------------------------------------------------------------
// HASH Get iterator
//------------------------------------------------------------------------------
QHash::Iterator QHash::getIterator(size_t count, const std::string &startCursor) {
  return QHash::Iterator(*this, count, startCursor);
}

//------------------------------------------------------------------------------
// HASH Iterator implementation
//------------------------------------------------------------------------------
QHash::Iterator::Iterator(QHash &qh, size_t cnt, const std::string &crs)
: qhash(qh), count(cnt), cursor(crs) {
  fillFromBackend();
}

bool QHash::Iterator::valid() const {
  return ! results.empty();
}

void QHash::Iterator::fillFromBackend() {
  while(!reachedEnd && results.empty()) {
    reqs++;
    std::pair<std::string, std::map<std::string, std::string> > answer =
    qhash.hscan(cursor, count);

    cursor = answer.first;
    results = std::move(answer.second);

    if(cursor == "0") {
      reachedEnd = true;
    }
  }
}

void QHash::Iterator::next() {
  if(!results.empty()) {
    results.erase(results.begin());
  }
  fillFromBackend();
}

std::string QHash::Iterator::getKey() const {
  return results.begin()->first;
}

std::string QHash::Iterator::getValue() const {
  return results.begin()->second;
}

size_t QHash::Iterator::requestsSoFar() const {
  return reqs;
}

QCLIENT_NAMESPACE_END
