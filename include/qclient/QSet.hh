//------------------------------------------------------------------------------
//! @file QSet.hh
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

#pragma once
#include "qclient/QClient.hh"
#include "qclient/Namespace.hh"
#include "qclient/Utils.hh"
#include <vector>
#include <set>


QCLIENT_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class QSet
//------------------------------------------------------------------------------
class QSet
{
public:
  //----------------------------------------------------------------------------
  //! Default constructor
  //----------------------------------------------------------------------------
  QSet() = default;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param cl qclient object used for sending requests
  //! @param set_key the key name for the hash
  //----------------------------------------------------------------------------
  QSet(QClient& cl, const std::string& set_key):
    mClient(&cl), mKey(set_key)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QSet()
  {
    mClient = nullptr;
  }

  //----------------------------------------------------------------------------
  //! Get the Set key
  //!
  //! @return string
  //----------------------------------------------------------------------------
  std::string getKey() const
  {
    return mKey;
  }

  //----------------------------------------------------------------------------
  //! Set the Set key
  //!
  //! @param new_key new key value
  //----------------------------------------------------------------------------
  void setKey(const std::string& new_key)
  {
    mKey = new_key;
  }

  //----------------------------------------------------------------------------
  //! Set Redox client object
  //!
  //! @param new_client new Redox client
  //!
  //----------------------------------------------------------------------------
  void setClient(QClient& new_client)
  {
    mClient = &new_client;
  }

  //----------------------------------------------------------------------------
  //! Redis SET add command - synchronous
  //!
  //! @param member value to be added to the set
  //!
  //! @return true if member added, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool sadd(const T& member);

  //----------------------------------------------------------------------------
  //! Redis SET add command - asynchronous
  //!
  //! @param member value to be added to the set
  //!
  //! @reutrn future object
  //----------------------------------------------------------------------------
  template <typename T>
  std::future<redisReplyPtr> sadd_async(const T& member);

  //----------------------------------------------------------------------------
  //! Redis SET add command for multiple members - synchronous
  //!
  //! @param vect_members values to be added to the set
  //! TODO: make vect_members rvalue ref to use move semantics
  //!
  //! @return number of elements added to the set
  //----------------------------------------------------------------------------
  // TODO: template the vector contents
  long long int sadd(std::vector<std::string> vect_members);

  //----------------------------------------------------------------------------
  //! Redis SET remove command - synchronous
  //!
  //! @param member value to be removed from the set
  //!
  //! @return true if member removed, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool srem(const T& member);

  //----------------------------------------------------------------------------
  //! Redis SET remove command - asynchronous
  //!
  //! @param member value to be removed from the set
  //!
  //! @return true if member removed, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  std::future<redisReplyPtr> srem_async(const T& member);

  //----------------------------------------------------------------------------
  //! Redis SET remove command for multiple members - synchronous
  //!
  //! @param vect_members values to be removed from the set
  //!
  //! @return number of elements removed from the set
  //----------------------------------------------------------------------------
  // TODO: template the vector contents
  long long int srem(std::vector<std::string> vect_members);

  //----------------------------------------------------------------------------
  //! Redis SET size command - synchronous
  //!
  //! @return size of the set
  //----------------------------------------------------------------------------
  long long int scard();

  //----------------------------------------------------------------------------
  //! Redis SET ismember command - synchronous
  //!
  //! @param member value to be searched in the set
  //!
  //! @return true if member in the set, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool sismember(const T& member);

  //----------------------------------------------------------------------------
  //! Redis SET members command - synchronous
  //!
  //! @return set containing the members of the set
  //----------------------------------------------------------------------------
  std::set<std::string> smembers();

  //----------------------------------------------------------------------------
  //! Redis SET SSCAN command - synchronous
  //!
  //! @param cursor cursor for current request
  //! @param count max number of elements to return
  //!
  //! @return pair representing the cursor value and a vector of the elements
  //!          returned in the current step
  //----------------------------------------------------------------------------
  std::pair< std::string, std::vector<std::string> >
  sscan(std::string cursor, long long count = 1000);

private:
  QClient* mClient; ///< Redox client object
  std::string mKey; ///< Key of the set object
};

//------------------------------------------------------------------------------
// Set related templated methods implementation
//------------------------------------------------------------------------------
template <typename T>
std::future<redisReplyPtr> QSet::sadd_async(const T& member)
{
  std::string smember = stringify(member);
  return mClient->execute({"SADD", mKey, smember});
}

template <typename T>
bool QSet::sadd(const T& member)
{
  auto future = sadd_async(member);
  redisReplyPtr reply = future.get();

  if (!reply) {
    throw std::runtime_error("[FATAL] Error sadd key: " + mKey + " member: "
			     + stringify(member) + ": No connection");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hset key: " + mKey + " field: "
			     + stringify(member) + ": Unexpected reply type: " +
			     std::to_string(reply->type));
  }

  return (reply->integer == 1);
}

template <typename T>
std::future<redisReplyPtr> QSet::srem_async(const T& member)
{
  std::string smember = stringify(member);
  return mClient->execute({"SREM", mKey, smember});
}

template <typename T>
bool QSet::srem(const T& member)
{
  auto future = srem_async(member);
  redisReplyPtr reply = future.get();

  if (!reply) {
    throw std::runtime_error("[FATAL] Error srem key: " + mKey + " member: "
			     + stringify(member) + ": No connection");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error srem key: " + mKey + " member: "
			     + stringify(member) + ": Unexpected reply type: " +
			     std::to_string(reply->type));
  }

  return (reply->integer == 1);
}

template <typename T>
bool QSet::sismember(const T& member)
{
  std::string smember = stringify(member);
  auto future = mClient->execute({"SISMEMBER", mKey, smember});
  redisReplyPtr reply = future.get();

  if (!reply) {
    throw std::runtime_error("[FATAL] Error sismember key: " + mKey + " member: "
			     + smember + ": No connection");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error sismember key: " + mKey + " member: "
			     + smember + ": Unexpected reply type: " +
			     std::to_string(reply->type));
  }

  return (reply->integer == 1);
}

QCLIENT_NAMESPACE_END