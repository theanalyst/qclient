// ----------------------------------------------------------------------
// File: queueing.cc
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
#include "qclient/queueing/ThreadSafeQueue.hh"
#include "qclient/queueing/AttachableQueue.hh"
#include "qclient/queueing/RingBuffer.hh"
#include "qclient/queueing/LastNSet.hh"
#include "qclient/queueing/LastNMap.hh"

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

TYPED_TEST_SUITE(Thread_Safe_Queue, Implementations);

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

class Accumulator {
public:

  void add(int &&val) {
    sum += val;
  }

  int getSum() const {
    return sum;
  }

private:
  int sum = 0;
};

TEST(AttachableQueue, BasicSanity) {
  AttachableQueue<int, 10> queue;

  // Detached mode, acts as a queue
  queue.emplace_back(3);
  ASSERT_EQ(queue.size(), 1u);
  ASSERT_EQ(queue.front(), 3);
  queue.pop_front();
  ASSERT_EQ(queue.size(), 0u);

  queue.emplace_back(4);
  queue.emplace_back(5);
  queue.emplace_back(5);

  ASSERT_EQ(queue.size(), 3u);
  ASSERT_EQ(queue.front(), 4);
  queue.pop_front();
  ASSERT_EQ(queue.size(), 2u);

  // Attach callback
  using std::placeholders::_1;

  Accumulator acu;
  queue.attach(std::bind(&Accumulator::add, &acu, _1));
  ASSERT_EQ(queue.size(), 0u);
  ASSERT_EQ(acu.getSum(), 10);

  queue.emplace_back(3);
  ASSERT_EQ(acu.getSum(), 13);
  ASSERT_EQ(queue.size(), 0u);

  queue.detach();
  queue.emplace_back(7);
  ASSERT_EQ(acu.getSum(), 13);
  ASSERT_EQ(queue.size(), 1u);
  ASSERT_EQ(queue.front(), 7);

  queue.attach(std::bind(&Accumulator::add, &acu, _1));
  ASSERT_EQ(queue.size(), 0u);
  ASSERT_EQ(acu.getSum(), 20);
}

TEST(RingBuffer, BasicSanity) {
  RingBuffer<std::string> ringBuffer(3);

  ASSERT_EQ(ringBuffer.getNextToEvict(), "");
  ASSERT_FALSE(ringBuffer.hasRolledOver());
  ringBuffer.emplace_back("aaa");

  ASSERT_EQ(ringBuffer.getNextToEvict(), "");
  ASSERT_FALSE(ringBuffer.hasRolledOver());
  ringBuffer.emplace_back("bbb");

  ASSERT_EQ(ringBuffer.getNextToEvict(), "");
  ASSERT_FALSE(ringBuffer.hasRolledOver());
  ringBuffer.emplace_back("ccc");

  ASSERT_EQ(ringBuffer.getNextToEvict(), "aaa");
  ASSERT_TRUE(ringBuffer.hasRolledOver());
  ringBuffer.emplace_back("ddd");

  ASSERT_EQ(ringBuffer.getNextToEvict(), "bbb");
  ASSERT_TRUE(ringBuffer.hasRolledOver());
  ringBuffer.emplace_back("eee");

  ASSERT_EQ(ringBuffer.getNextToEvict(), "ccc");
  ASSERT_TRUE(ringBuffer.hasRolledOver());
  ringBuffer.emplace_back("eee");

  ASSERT_EQ(ringBuffer.getNextToEvict(), "ddd");
  ASSERT_TRUE(ringBuffer.hasRolledOver());
}

TEST(LastNSet, BasicSanity) {
  LastNSet<std::string> lastSet(3);

  ASSERT_FALSE(lastSet.query(""));

  lastSet.emplace("aaa");
  ASSERT_TRUE(lastSet.query("aaa"));
  ASSERT_FALSE(lastSet.query("bbb"));
  ASSERT_FALSE(lastSet.query("ccc"));

  lastSet.emplace("bbb");
  ASSERT_TRUE(lastSet.query("aaa"));
  ASSERT_TRUE(lastSet.query("bbb"));
  ASSERT_FALSE(lastSet.query("ccc"));

  lastSet.emplace("ccc");
  ASSERT_TRUE(lastSet.query("aaa"));
  ASSERT_TRUE(lastSet.query("bbb"));
  ASSERT_TRUE(lastSet.query("ccc"));

  lastSet.emplace("ddd");
  ASSERT_FALSE(lastSet.query("aaa"));
  ASSERT_TRUE(lastSet.query("bbb"));
  ASSERT_TRUE(lastSet.query("ccc"));
  ASSERT_TRUE(lastSet.query("ddd"));

  ASSERT_FALSE(lastSet.query(""));
}

TEST(LastNMap, BasicSanity) {
  LastNMap<std::string, int32_t> lastMap(3);

  lastMap.insert("a", 99);
  int32_t val;

  ASSERT_TRUE(lastMap.query("a", val));
  ASSERT_EQ(val, 99);

  lastMap.insert("a", 88);

  ASSERT_TRUE(lastMap.query("a", val));
  ASSERT_EQ(val, 88);

  lastMap.insert("b", 77);

  ASSERT_TRUE(lastMap.query("a", val));
  ASSERT_EQ(val, 88);

  ASSERT_TRUE(lastMap.query("b", val));
  ASSERT_EQ(val, 77);

  lastMap.insert("c", 66);

  ASSERT_TRUE(lastMap.query("a", val));
  ASSERT_EQ(val, 88);

  ASSERT_TRUE(lastMap.query("b", val));
  ASSERT_EQ(val, 77);

  ASSERT_TRUE(lastMap.query("c", val));
  ASSERT_EQ(val, 66);

  lastMap.insert("d", 55);

  ASSERT_FALSE(lastMap.query("a", val));

  ASSERT_TRUE(lastMap.query("b", val));
  ASSERT_EQ(val, 77);

  ASSERT_TRUE(lastMap.query("c", val));
  ASSERT_EQ(val, 66);

  ASSERT_TRUE(lastMap.query("d", val));
  ASSERT_EQ(val, 55);
}
