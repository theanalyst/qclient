//------------------------------------------------------------------------------
// File: test-config.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
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

#include <climits>
#include "test-config.hh"
TestConfig testconfig;

static bool startswith(const std::string &str, const std::string &prefix) {
  if(prefix.size() > str.size()) return false;

  for(size_t i = 0; i < prefix.size(); i++) {
    if(str[i] != prefix[i]) return false;
  }
  return true;
}

static std::vector<std::string> split(std::string data, std::string token) {
    std::vector<std::string> output;
    size_t pos = std::string::npos;
    do {
        pos = data.find(token);
        output.push_back(data.substr(0, pos));
        if(std::string::npos != pos)
            data = data.substr(pos + token.size());
    } while (std::string::npos != pos);
    return output;
}

static bool my_strtoll(const std::string &str, int64_t &ret) {
  char *endptr = NULL;
  ret = strtoll(str.c_str(), &endptr, 10);
  if(endptr != str.c_str() + str.size() || ret == LLONG_MIN || ret == LONG_LONG_MAX) {
    return false;
  }
  return true;
}

// parse environment variables to give the possibility to override defaults
TestConfig::TestConfig() {
  int i = 1;
  char *s = *environ;

  for (; s; i++) {
    std::string var(s);
    if(startswith(var, "QCL_TESTS_")) {
      std::vector<std::string> chunks = split(var, "=");
      if(chunks.size() != 2) {
        std::cerr << "Could not parse environment variable: " << var << std::endl;
        exit(EXIT_FAILURE);
      }

      parseSingle(chunks[0], chunks[1]);
    }
    s = *(environ+i);
  }

  if(tlsconfig.certificatePath.empty() != tlsconfig.keyPath.empty()) {
    std::cerr << "Both QCL_TESTS_TLS_CERT and QCL_TESTS_TLS_KEY must be supplied." << std::endl;
    exit(EXIT_FAILURE);
  }

  if(!tlsconfig.certificatePath.empty()) {
    tlsconfig.active = true;
  }
}

void TestConfig::parseSingle(const std::string &key, const std::string &value) {
  if(key == "QCL_TESTS_HOST") {
    host = value;
  }
  else if(key == "QCL_TESTS_PORT") {
    int64_t tmp;
    if(!my_strtoll(value, tmp)) {
      std::cerr << "Could not parse '" << key << "'" << std::endl;
      exit(EXIT_FAILURE);
    }

    port = tmp;
  }
  else if(key == "QCL_TESTS_TLS_CERT") {
    tlsconfig.certificatePath = value;
  }
  else if(key == "QCL_TESTS_TLS_KEY") {
    tlsconfig.keyPath = value;
  }
  else {
    std::cerr << "Unknown configuration option: " << key << " => " << value << std::endl;
    exit(EXIT_FAILURE);
  }
  std::cerr << "Applying configuration option: " << key << " => " << value << std::endl;
}
