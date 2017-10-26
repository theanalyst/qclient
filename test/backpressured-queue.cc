// ----------------------------------------------------------------------
// File: backpressured-queue.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#include "gtest/gtest.h"
#include "qclient/QClient.hh"
#include <functional>
#include "qclient/BackpressuredQueue.hh"

using namespace qclient;


TEST(BackpressureStrategy, limit_size) {
  BackpressureStrategyLimitSize strategy(3);
  ASSERT_TRUE(strategy.pushEvent(5));
  ASSERT_TRUE(strategy.pushEvent(5));
  ASSERT_TRUE(strategy.pushEvent(9));
  ASSERT_FALSE(strategy.pushEvent(1));
  ASSERT_TRUE(strategy.popEvent(5));
}

class Stopwatch {
public:
  Stopwatch() : start(std::chrono::high_resolution_clock::now()) {}

  std::chrono::milliseconds measure() {
    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
  }

private:
  std::chrono::high_resolution_clock::time_point start;
};

template<typename Duration, typename Func>
void delayedRunner(Duration duration, Func func) {
  std::this_thread::sleep_for(duration);
  func();
}

class AsynchronousHelper {
public:
  AsynchronousHelper() {}

  ~AsynchronousHelper() {
    while(!threads.empty()) {
      join();
    }
  }

  template<typename Duration, typename... Args>
  void launch(Duration duration, Args&&... args) {
    auto func = std::bind(std::forward<Args>(args)...);
    threads.emplace_back(&delayedRunner<Duration, decltype(func)>, duration, func);
  }

  void join() {
    threads.front().join();
    threads.pop_front();
  }

private:
  std::list<std::thread> threads;
};

#define ASSERT_BLOCKED(st, result, duration) {                  \
  PushStatus status = st; ASSERT_EQ(status.ok, result);         \
  ASSERT_NE(status.blockedFor, std::chrono::milliseconds(0));   \
  if(result) {                                                  \
    ASSERT_LE(status.blockedFor, duration);                     \
  }                                                             \
  else {                                                        \
    ASSERT_GE(status.blockedFor, duration);                     \
  }                                                             \
}

#define ASSERT_NOT_BLOCKED(st, result) { PushStatus status = st; ASSERT_EQ(status.ok, result); ASSERT_EQ(status.blockedFor, std::chrono::milliseconds(0)); }

TEST(BackpressuredQueue, basic_sanity) {
  BackpressuredQueue<int, BackpressureStrategyLimitSize> queue(nullptr, 4);
  AsynchronousHelper helper;

  // A few successful operations..
  ASSERT_NOT_BLOCKED(queue.push(5), true);
  ASSERT_NOT_BLOCKED(queue.push(4), true);
  ASSERT_NOT_BLOCKED(queue.push(3), true);
  ASSERT_NOT_BLOCKED(queue.push(2), true);

  // Limit was reached, no blocking
  ASSERT_NOT_BLOCKED(queue.push(1, std::chrono::milliseconds(0)), false);

  ASSERT_EQ(queue.top(), 5);
  ASSERT_EQ(queue.top(), 5);

  // Remove an item in 15 ms..
  helper.launch(std::chrono::milliseconds(15), &decltype(queue)::pop, &queue);

  // Maximum wait limit exceeded
  Stopwatch stopwatch;
  ASSERT_BLOCKED(queue.push(1, std::chrono::milliseconds(5)), false, std::chrono::milliseconds(5));
  ASSERT_GE(stopwatch.measure(), std::chrono::milliseconds(5));
  ASSERT_LE(stopwatch.measure(), std::chrono::milliseconds(20));

  // Wait until pop happens, add another item
  helper.join();
  ASSERT_EQ(queue.top(), 4);
  ASSERT_NOT_BLOCKED(queue.push(0), true);

  // Remove another item in 15 ms..
  helper.launch(std::chrono::milliseconds(15), &decltype(queue)::pop, &queue);
  stopwatch = Stopwatch();
  ASSERT_BLOCKED(queue.push(-1, std::chrono::milliseconds(50)), true, std::chrono::milliseconds(40));
  ASSERT_GE(stopwatch.measure(), std::chrono::milliseconds(10));
  helper.join();

  ASSERT_EQ(queue.top(), 3);
  queue.pop();
  ASSERT_EQ(queue.top(), 2);
  queue.pop();
  ASSERT_EQ(queue.top(), 0);
  queue.pop();
  ASSERT_EQ(queue.size(), 1);
  ASSERT_EQ(queue.top(), -1);
  queue.pop();
}
