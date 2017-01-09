//------------------------------------------------------------------------------
//! @file QHash.hh
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
#include "qclient/Namespace.hh"
#include "qclient/QClient.hh"
#include "qclient/Utils.hh"
#include <unordered_map>

QCLIENT_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class QHash
//------------------------------------------------------------------------------
class QHash
{
public:
  //----------------------------------------------------------------------------
  //! Default constructor
  //----------------------------------------------------------------------------
  QHash() = default;

  //----------------------------------------------------------------------------
  //!  Constructor
  //!
  //! @param fs XRootD fs client object used for sending requests
  //! @param hash_key the key name for the hash
  //! @return string
  //----------------------------------------------------------------------------
  QHash(QClient& cl, const std::string& hash_key):
    mClient(&cl), mKey(hash_key)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QHash()
  {
    mClient = NULL;
  }

  //----------------------------------------------------------------------------
  //!Get the Hash key
  //!
  //!@return string
  //----------------------------------------------------------------------------
  std::string getKey() const
  {
    return mKey;
  }

  //----------------------------------------------------------------------------
  //!Set the Hash key
  //!
  //!@param new_key new key value
  //----------------------------------------------------------------------------
  void setKey(const std::string& new_key)
  {
    mKey = new_key;
  }

  //----------------------------------------------------------------------------
  //!Set XRootD fs client object
  //!
  //!@param new_client new XRootD fs client
  //----------------------------------------------------------------------------
  void setClient(QClient& new_client)
  {
    mClient = &new_client;
  }

  //----------------------------------------------------------------------------
  //! HASH get command - synchronous
  //!
  //! @param field hash field
  //!
  //! @return return the value associated with "field" in the hash stored at "key".
  //!         If no such key exists then it return an empty string.
  //----------------------------------------------------------------------------
  std::string hget(const std::string& field);

  //----------------------------------------------------------------------------
  //! HASH set command - synchronous
  //!
  //! @param field hash field
  //! @param value value to set
  //!
  //! @return return true if successful, otherwise false
  //----------------------------------------------------------------------------
  template <typename T>
  bool hset(const std::string& field, const T& value);

  //----------------------------------------------------------------------------
  //! HASH set command - asynchronous
  //!
  //! @param field hash field
  //! @param value value to set
  //!
  //! @return return future object
  //----------------------------------------------------------------------------
  template <typename T> std::future<redisReplyPtr>
  hset_async(const std::string& field, const T& value);

  //----------------------------------------------------------------------------
  //! HASH set if doesn't exist command - synchronous
  //!
  //! @param field hash field
  //! @param value value to set
  //!
  //! @return true if value set, otherwise false meaning the value existed and
  //!         no operation was performed
  //----------------------------------------------------------------------------
  template <typename T>
  bool hsetnx(const std::string& field, const T& value);

  //----------------------------------------------------------------------------
  //! HASH del command - synchronous
  //!
  //! @param field hash field
  //!
  //! @return true if field removed, otherwise false
  //----------------------------------------------------------------------------
  bool hdel(const std::string& field);

  //----------------------------------------------------------------------------
  //! HASH del command - asynchronous
  //!
  //! @param field hash field
  //!
  //! @return future object
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> hdel_async(const std::string& field);

  //----------------------------------------------------------------------------
  //! HASH get all command - synchronous
  //!
  //! @return list of fields and their values stored in the hash, or an empty
  //!         list when key does not exist
  //----------------------------------------------------------------------------
  std::vector<std::string> hgetall();

  //----------------------------------------------------------------------------
  //! HASH exists command - synchronous
  //!
  //! @param field hash field
  //!
  //! @return true if fields is in map, otherwise false
  //----------------------------------------------------------------------------
  bool hexists(const std::string& field);

  //----------------------------------------------------------------------------
  //! HASH length command - synchronous
  //!
  //! @return length of the has
  //----------------------------------------------------------------------------
  long long int hlen();

  //----------------------------------------------------------------------------
  //! HASH length command - asynchronous
  //!
  //! @return future containing the response
  //----------------------------------------------------------------------------
  std::future<redisReplyPtr> hlen_async();

  //----------------------------------------------------------------------------
  //! HASH increment_by command - synchronous
  //!
  //! @param key name of the hash
  //! @param field hash field
  //! @param increment value to increment by
  //!
  //! @return the value at "field" after the increment operation
  //----------------------------------------------------------------------------
  template <typename T>
  long long int
  hincrby(const std::string& field, const T& increment);

  //----------------------------------------------------------------------------
  //! HASH increment_by_float command - synchronous
  //!
  //!@param field hash field
  //!@param increment value to increment by
  //!
  //!@return the value at "field" after the increment operation
  //----------------------------------------------------------------------------
  template <typename T>
  double
  hincrbyfloat(const std::string& field, const T& increment);

  //----------------------------------------------------------------------------
  //! HASH keys command - synchronous
  //!
  //! @return vector of fields in the hash
  //----------------------------------------------------------------------------
  std::vector<std::string> hkeys();

  //----------------------------------------------------------------------------
  //! HASH values command - synchronous
  //!
  //! @return vector of values in the hash
  //----------------------------------------------------------------------------
  std::vector<std::string> hvals();

  //----------------------------------------------------------------------------
  //! HASH SCAN command - synchronous
  //!
  //! @param cursor cursor for current request
  //! @param count max number of elements to return
  //!
  //! @return pair representing the cursor value and a map of the elements
  //!         returned in the current step
  //-----------------------------------------------------------------------------
  std::pair<std::string, std::unordered_map<std::string, std::string> >
  hscan(const std::string& cursor, long long count = 1000);

private:
  QClient* mClient; ///< Client to talk to the backend
  std::string mKey; ///< Key of the hash object
};

//------------------------------------------------------------------------------
// HSET operation - asynchronous
//------------------------------------------------------------------------------
template <typename T>
std::future<redisReplyPtr>
QHash::hset_async(const std::string& field, const T& value)
{
  std::string svalue = stringify(value);
  return mClient->execute({"HSET", mKey, field, svalue});
}

//------------------------------------------------------------------------------
// HSET operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
bool
QHash::hset(const std::string& field, const T& value)
{
  auto future = hset_async(field, value);
  redisReplyPtr reply = future.get();

  if (!reply) {
    throw std::runtime_error("[FATAL] Error hset key: " + mKey + " field: "
			     + field + ": No connection");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hset key: " + mKey + " field: "
			     + field + ": Unexpected reply type: " +
			     std::to_string(reply->type));
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HSETNX operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
bool QHash::hsetnx(const std::string& field, const T& value)
{
  std::string svalue = stringify(value);
  auto future = mClient->execute({"HSETNX", mKey, field, svalue});
  redisReplyPtr reply = future.get();

  if (!reply) {
    throw std::runtime_error("[FATAL] Error hsetnx key: " + mKey + " field: "
			     + field + ": No connection");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hsetnx key: " + mKey + " field: "
			     + field + ": Unexpected reply type: " +
			     std::to_string(reply->type));
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HINCRBY operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
long long int QHash::hincrby(const std::string& field, const T& increment)
{
  std::string sincrement = stringify(increment);
  auto future = mClient->execute({"HINCRBY", mKey, field, sincrement});
  redisReplyPtr reply = future.get();

  if (!reply) {
    throw std::runtime_error("[FATAL] Error hincrby key: " + mKey + " field: "
			     + field + ": No connection");
  }

  if (reply->type != REDIS_REPLY_INTEGER) {
    throw std::runtime_error("[FATAL] Error hincrby key: " + mKey + " field: "
			     + field + ": Unexpected reply type: " +
			     std::to_string(reply->type));
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// HINCRBYFLOAT operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
double QHash::hincrbyfloat(const std::string& field, const T& increment)
{
  std::string sincrement = stringify(increment);
  auto future = mClient->execute({"HINCRBYFLOAT", mKey, field, sincrement});
  redisReplyPtr reply = future.get();

  if (!reply) {
    throw std::runtime_error("[FATAL] Error hincrbyfloat key: " + mKey + " field: "
			     + field + ": No connection");
  }

  if (reply->type != REDIS_REPLY_STRING) {
    throw std::runtime_error("[FATAL] Error hincrbyfloat key: " + mKey + " field: "
			     + field + " : Unexpected reply type: " +
			     std::to_string(reply->type));
  }

  std::string resp{reply->str, (size_t)reply->len};
  return std::stod(resp);
}

QCLIENT_NAMESPACE_END
