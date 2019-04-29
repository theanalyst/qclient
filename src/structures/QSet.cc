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

#include "qclient/structures/QSet.hh"

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
  redisReplyPtr reply = mClient->execute(lst_elem).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error sadd key: " + mKey +
                             " with multiple members: Unexpected/null reply");
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
  redisReplyPtr reply = mClient->execute(lst_elem).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error srem key: " + mKey +
                             " with multiple members: Unexpected/null reply");
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Redis SET size command - synchronous
//------------------------------------------------------------------------------
long long int QSet::scard()
{
  redisReplyPtr reply = mClient->exec("SCARD", mKey).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error scard key: " + mKey +
                             " : Unexpected/null reply");
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// Redis SET smembers command - synchronous
//------------------------------------------------------------------------------
std::set<std::string> QSet::smembers()
{
  redisReplyPtr reply = mClient->exec("SMEMBERS", mKey).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_ARRAY)) {
    throw std::runtime_error("[FATAL] Error smembers key: " + mKey +
                             " : Unexpected/null reply");
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
QSet::sscan(const std::string &cursor, long long count)
{
  redisReplyPtr reply = mClient->exec("SSCAN", mKey, cursor, "COUNT", fmt::to_string(count)).get();

  if (reply == nullptr) {
    throw std::runtime_error("[FATAL] Error sscan key: " + mKey +
                             ": Unexpected/null reply");
  }

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
void
QSet::sadd_async(const std::set<std::string>& set_elem, AsyncHandler* ah)
{
  std::vector<std::string> cmd;
  cmd.reserve(set_elem.size() + 2);
  (void) cmd.push_back("SADD");
  (void) cmd.push_back(mKey);
  (void) cmd.insert(cmd.end(), set_elem.begin(), set_elem.end());
  ah->Register(mClient, cmd);
}

//------------------------------------------------------------------------------
// Redis SET add command for multiple elements - asynchronous
//------------------------------------------------------------------------------
void
QSet::sadd_async(const std::list<std::string>& set_elem, AsyncHandler* ah)
{
  std::vector<std::string> cmd;
  cmd.reserve(set_elem.size() + 2);
  (void) cmd.push_back("SADD");
  (void) cmd.push_back(mKey);
  (void) cmd.insert(cmd.end(), set_elem.begin(), set_elem.end());
  ah->Register(mClient, cmd);
}

//------------------------------------------------------------------------------
// SET Get iterator
//------------------------------------------------------------------------------
QSet::Iterator QSet::getIterator(size_t count, const std::string &startCursor) {
  return QSet::Iterator(*this, count, startCursor);
}

//------------------------------------------------------------------------------
// SET Iterator implementation
//------------------------------------------------------------------------------
QSet::Iterator::Iterator(QSet &qs, size_t cnt, const std::string &crs)
: qset(qs), count(cnt), cursor(crs) {
  fillFromBackend();
}

bool QSet::Iterator::valid() const {
  return it != results.end();
}

void QSet::Iterator::fillFromBackend() {
  while(!reachedEnd && it == results.end()) {
    reqs++;

    std::pair<std::string, std::vector<std::string> > answer =
    qset.sscan(cursor, count);

    cursor = answer.first;
    results = std::move(answer.second);
    it = results.begin();

    if(cursor == "0") {
      reachedEnd = true;
    }
  }
}

void QSet::Iterator::next() {
  if(reachedEnd && it == results.end()) {
    // No more elements, clear result vector
    results.clear();
    it = results.begin();
    return;
  }

  if(it != results.end()) {
    // Safe to progress iterator
    it++;
  }

  if(it == results.end()) {
    // Reached the end?
    fillFromBackend();
  }
}

std::string QSet::Iterator::getElement() const {
  return *it;
}

size_t QSet::Iterator::requestsSoFar() const {
  return reqs;
}

QCLIENT_NAMESPACE_END
