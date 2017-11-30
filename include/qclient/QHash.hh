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
#include "qclient/AsyncHandler.hh"
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
  QHash(): mClient(nullptr) {}

  //----------------------------------------------------------------------------
  //!  Constructor
  //!
  //! @param fs qclient object used for sending requests
  //! @param hash_key the key name for the hash
  //----------------------------------------------------------------------------
  QHash(QClient& cl, const std::string& hash_key):
    mClient(&cl), mKey(hash_key)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QHash()
  {
    mClient = nullptr;
  }

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  QHash(const QHash& other);

  //----------------------------------------------------------------------------
  //! Copy assignment
  //----------------------------------------------------------------------------
  QHash& operator=(const QHash& other);

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  QHash(QHash&& other) = delete;

  //----------------------------------------------------------------------------
  //! Move assignment
  //----------------------------------------------------------------------------
  QHash& operator=(QHash&& other) = default;

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
  //! Set the Hash key
  //!
  //!@param new_key new key value
  //----------------------------------------------------------------------------
  void setKey(const std::string& new_key)
  {
    mKey = new_key;
  }

  //----------------------------------------------------------------------------
  //! Set client object
  //!
  //! @param new_client new XRootD fs client
  //----------------------------------------------------------------------------
  void setClient(QClient& new_client)
  {
    mClient = &new_client;
  }

  //----------------------------------------------------------------------------
  //! Get client object
  //----------------------------------------------------------------------------
  inline QClient* getClient()
  {
    return mClient;
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
  //! @param ah async handler
  //----------------------------------------------------------------------------
  template <typename T>
  void hset_async(const std::string& field, const T& value, AsyncHandler* ah);

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
  //! HASH multi set command - synchronous
  //!
  //! @param lst_elem elements to set in key-value "pairs"
  //!
  //! @return return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool hmset(std::list<std::string> lst_elem);

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
  //! @param ah async handler
  //----------------------------------------------------------------------------
  void
  hdel_async(const std::string& field, AsyncHandler* ah);

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
  //! @param ah async handler
  //----------------------------------------------------------------------------
  void
  hlen_async(AsyncHandler* ah);

  //----------------------------------------------------------------------------
  //! HASH increment_by command - synchronous
  //!
  //! @param field hash field
  //! @param increment value to increment by
  //!
  //! @return the value at "field" after the increment operation
  //----------------------------------------------------------------------------
  template <typename T>
  long long int
  hincrby(const std::string& field, const T& increment);

  //----------------------------------------------------------------------------
  //! HASH increment_by command - asynchronous
  //!
  //! @param field hash field
  //! @param increment value to increment by
  //! @param ah async handler
  //----------------------------------------------------------------------------
  template <typename T>
  void
  hincrby_async(const std::string& field, const T& increment, AsyncHandler* ah);

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
void
QHash::hset_async(const std::string& field, const T& value, AsyncHandler* ah)
{
  ah->Register(mClient, {"HSET", mKey, field, stringify(value)});
}

//------------------------------------------------------------------------------
// HSET operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
bool
QHash::hset(const std::string& field, const T& value)
{
  redisReplyPtr reply = mClient->exec("HSET", mKey, field, stringify(value)).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error hset key: " + mKey + " field: "
                             + field + ": Unexpected/null reply");
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HSETNX operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
bool QHash::hsetnx(const std::string& field, const T& value)
{
  redisReplyPtr reply = mClient->exec("HSETNX", mKey, field, stringify(value)).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error hsetnx key: " + mKey + " field: "
                             + field + ": Unexpected/null reply");
  }

  return (reply->integer == 1);
}

//------------------------------------------------------------------------------
// HINCRBY operation - asynchronous
//------------------------------------------------------------------------------
template <typename T>
void
QHash::hincrby_async(const std::string& field, const T& increment,
                     AsyncHandler* ah)
{
  ah->Register(mClient, {"HINCRBY", mKey, field, stringify(increment)});
}

//------------------------------------------------------------------------------
// HINCRBY operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
long long int QHash::hincrby(const std::string& field, const T& increment)
{
  redisReplyPtr reply = mClient->exec("HINCRBY", mKey, field,
                                      stringify(increment)).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_INTEGER)) {
    throw std::runtime_error("[FATAL] Error hincrby key: " + mKey + " field: "
                             + field + ": Unexpected/null reply");
  }

  return reply->integer;
}

//------------------------------------------------------------------------------
// HINCRBYFLOAT operation - synchronous
//------------------------------------------------------------------------------
template <typename T>
double QHash::hincrbyfloat(const std::string& field, const T& increment)
{
  redisReplyPtr reply = mClient->exec("HINCRBYFLOAT", mKey, field,
                                      stringify(increment)).get();

  if ((reply == nullptr) || (reply->type != REDIS_REPLY_STRING)) {
    throw std::runtime_error("[FATAL] Error hincrbyfloat key: " + mKey + " field: "
                             + field + " : Unexpected/null reply");
  }

  std::string resp{reply->str, (size_t)reply->len};
  return std::stod(resp);
}

QCLIENT_NAMESPACE_END
