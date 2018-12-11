// ----------------------------------------------------------------------
// File: thread-safe-queue.cc
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

#include <gtest/gtest.h>
#include "qclient/ThreadSafeQueue.hh"

using namespace qclient;

class Coord {
public:
  Coord() {}
  Coord(int a, int b) : x(a), y(b) {}
  int x = -1;
  int y = -1;
};


template <class T>
class Thread_Safe_Queue : public testing::Test {
protected:
  T queue;
};

typedef ::testing::Types<
  ThreadSafeQueue<Coord, 1>, ThreadSafeQueue<Coord, 2>, ThreadSafeQueue<Coord, 3>,
  ThreadSafeQueue<Coord, 4>, ThreadSafeQueue<Coord, 5>, ThreadSafeQueue<Coord, 6>,
  ThreadSafeQueue<Coord, 7>, ThreadSafeQueue<Coord, 8>, ThreadSafeQueue<Coord, 9>,
  ThreadSafeQueue<Coord, 10>, ThreadSafeQueue<Coord, 11>, ThreadSafeQueue<Coord, 13>,
  ThreadSafeQueue<Coord, 16>, ThreadSafeQueue<Coord, 20>, ThreadSafeQueue<Coord, 32>,
  ThreadSafeQueue<Coord, 100>, ThreadSafeQueue<Coord, 128>, ThreadSafeQueue<Coord, 200>,
  ThreadSafeQueue<Coord, 333>> Implementations;

TYPED_TEST_CASE(Thread_Safe_Queue, Implementations);

TYPED_TEST(Thread_Safe_Queue, BasicSanity) {
  ASSERT_TRUE(this->queue.empty());
  ASSERT_EQ(this->queue.getNextSequenceNumber(), 0);
  ASSERT_EQ(0, this->queue.emplace_back(1, 2));
  ASSERT_EQ(this->queue.getNextSequenceNumber(), 1);
  ASSERT_FALSE(this->queue.empty());
  auto it = this->queue.begin();
  ASSERT_EQ(it.seq(), 0);

  Coord *coord = &it.item();
  ASSERT_EQ(coord->x, 1);
  ASSERT_EQ(coord->y, 2);

  it.next();
  ASSERT_EQ(it.seq(), 1);

  ASSERT_EQ(0, this->queue.pop_front());
  ASSERT_TRUE(this->queue.empty());

  ASSERT_EQ(1, this->queue.emplace_back(2, 3));
  ASSERT_EQ(this->queue.getNextSequenceNumber(), 2);
  ASSERT_FALSE(this->queue.empty());

  coord = &it.item();
  ASSERT_EQ(coord->x, 2);
  ASSERT_EQ(coord->y, 3);
  it.next();
  ASSERT_EQ(it.seq(), 2);
  ASSERT_EQ(1, this->queue.pop_front());
  ASSERT_TRUE(this->queue.empty());

  for(int i = 0; i < 100; i++) {
    ASSERT_EQ(2 + i, this->queue.emplace_back(i*i, i*i+1));
    ASSERT_EQ(this->queue.getNextSequenceNumber(), i+3);
    ASSERT_FALSE(this->queue.empty());
  }

  for(int i = 0; i < 100; i++) {
    coord = &it.item();
    ASSERT_EQ(coord->x, i*i);
    ASSERT_EQ(coord->y, i*i+1);
    it.next();
    ASSERT_EQ(it.seq(), 3+i);
    ASSERT_EQ(2+i, this->queue.pop_front());
  }

  ASSERT_TRUE(this->queue.empty());
}
