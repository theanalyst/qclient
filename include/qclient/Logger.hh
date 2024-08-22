//------------------------------------------------------------------------------
// File: Logger.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef QCLIENT_LOGGER_HH
#define QCLIENT_LOGGER_HH

#include <atomic>
#include <mutex>
#include <iostream>
#include <time.h>
#include <sys/time.h>

namespace qclient {


//------------------------------------------------------------------------------
//! Loglevel
//------------------------------------------------------------------------------
enum class LogLevel {
  kFatal = 0,
  kError = 1,
  kWarn  = 2,
  kInfo  = 3,
  kDebug = 4
};

//------------------------------------------------------------------------------
//! Logger object for use by QClient. By default, a global instance is used
//! which simply prints to std::cerr, but it's possible to override by
//! extending this class, and passing an object to qclient::Options.
//------------------------------------------------------------------------------
class Logger {
public:
  Logger() : logLevel(LogLevel::kInfo) {}

  virtual ~Logger() = default;

  virtual void print(LogLevel level, int line, const std::string &file, const std::string &msg) = 0;

  static std::string logLevelToString(LogLevel level) {
    switch(level) {
      case LogLevel::kFatal: return "FATAL";
      case LogLevel::kError: return "ERROR";
      case LogLevel::kWarn:  return "WARN";
      case LogLevel::kInfo:  return "INFO";
      case LogLevel::kDebug: return "DEBUG";
    }

    return "UNKNOWN";
  }

  LogLevel getLogLevel() const {
      return logLevel;
  }

  void setLogLevel(LogLevel level) {
      logLevel = level;
  }

  //----------------------------------------------------------------------------
  //! Get current timestamp for printing in the logs using standard C library
  //! calls and snprintf for formatting. Not thread-safe and should be
  //! called from a protected scope.
  //!
  //! @return string represenation of the timestamp
  //----------------------------------------------------------------------------
  std::string getLibcTimestamp() {
    tm tm;
    struct timeval tv;
    struct timezone tz;
    gettimeofday(&tv,&tz);
    time_t current_time= tv.tv_sec;
    localtime_r(&current_time, &tm);
    constexpr uint sz = 42;
    static char buffer[sz];
    snprintf(buffer, sz,
            "%02d%02d%02d %02d:%02d:%02d time=%lu.%06lu ",
            tm.tm_year - 100, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
            tm.tm_sec, current_time, (unsigned long) tv.tv_usec);
    return std::string(buffer);
  }



protected:
  std::atomic<LogLevel> logLevel;
};

//------------------------------------------------------------------------------
//! Logger object writing into std::cerr.
//------------------------------------------------------------------------------
class StandardErrorLogger : public Logger {
public:

  void print(LogLevel level, int line, const std::string &file, const std::string &msg) override {
    std::lock_guard<std::mutex> lock(mtx);
    std::cerr << getLibcTimestamp() << "[QCLIENT - " << logLevelToString(level) << " - "
              << file << ":" << line << "] " << msg << std::endl;
  }

private:
  std::mutex mtx;
};

//------------------------------------------------------------------------------
//! Why the ugly macro? Because by putting the if condition externally, we avoid
//! evaluating the function arguments for "print" if the loglevel is lower
//! than a particular message.
//------------------------------------------------------------------------------
#define QCLIENT_LOG(logger, logLevel, msg) if(logger && int(logger->getLogLevel()) >= int(logLevel)) { logger->print(logLevel, __LINE__, __FUNCTION__, static_cast<std::ostringstream&>(std::ostringstream().flush() << msg).str()); }

}

#endif
