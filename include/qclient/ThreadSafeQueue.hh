//------------------------------------------------------------------------------
// File: ThreadSafeQueue.hh
// Author: Georgios Bitzes - CERN
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

#ifndef QCLIENT_THREAD_SAFE_QUEUE_H
#define QCLIENT_THREAD_SAFE_QUEUE_H

#include <vector>
#include <memory>
#include <mutex>

namespace qclient {

//------------------------------------------------------------------------------
// A relatively efficient thread-safe queue, with the following use in mind:
// - Pushers should not block poppers, and vice versa.
// - Ability to scan through the queue, starting from the beginning, using a
//   long lived iterator.
// - Obtaining the iterator should be infrequent.
// - Progressing the iterator should be very fast, and lockless.
//
// The items are laid out in memory inside a singly-linked list composed of
// large memory chunks.
//
// This class does no error checking that it is being used correctly, and will
// blow up if:
// - Popping non-existent items.
// - Progressing the iterator past the end of the queue. An iterator is always
//   assumed to be valid.
//------------------------------------------------------------------------------

template<typename T, size_t BlockSize>
struct MemoryBlock {
  std::array<T, BlockSize> contents;
  std::unique_ptr<MemoryBlock> next;
};

template<typename T, size_t BlockSize>
class ThreadSafeQueue {
public:
  ThreadSafeQueue() {
    reset();
  }

  //----------------------------------------------------------------------------
  // Reset all contents, and start sequence numbers from 0 again.
  //----------------------------------------------------------------------------
  void reset() {
    frontSequenceNumber = 0;
    nextSequenceNumber = 0;

    firstBlockNextToPop = 0;
    lastBlockNextPos = 0;

    root.reset();
    lastBlock = nullptr;

    //--------------------------------------------------------------------------
    // Allocate root.
    //--------------------------------------------------------------------------
    root = std::unique_ptr<MemoryBlock<T, BlockSize>>(new MemoryBlock<T, BlockSize>());
    lastBlock = root.get();
  }

  //----------------------------------------------------------------------------
  // Constructs an item inside the queue, and returns that item's unique
  // sequence number.
  //----------------------------------------------------------------------------
  template<typename... Args>
  int64_t emplace_back(Args&&... args) {
    std::lock_guard<std::mutex> lock(pushMutex);

    new (&lastBlock->contents[lastBlockNextPos]) T(std::forward<Args>(args)...);
    lastBlockNextPos++;

    if(lastBlockNextPos == BlockSize) {
      allocateBlock();
    }

    return nextSequenceNumber++;
  }

  //----------------------------------------------------------------------------
  // Returns a reference to the top item.
  //----------------------------------------------------------------------------
  T& front() {
    std::lock_guard<std::mutex> lock(popMutex);
    return root->contents[firstBlockNextToPop];
  }

  //----------------------------------------------------------------------------
  // Pops an item from the queue, and returns the item's unique sequence number.
  //----------------------------------------------------------------------------
  int64_t pop_front() {
    std::lock_guard<std::mutex> lock(popMutex);

    firstBlockNextToPop++;
    if(firstBlockNextToPop == BlockSize) {
      removeRoot();
    }

    return frontSequenceNumber++;
  }

  class Iterator {
  public:
    Iterator() {}

    Iterator(MemoryBlock<T, BlockSize> *block, size_t pos, int64_t seq)
    : currentBlock(block), nextPos(pos), sequenceNumber(seq) {}

    T& item() {
      return currentBlock->contents[nextPos];
    }

    int64_t seq() const {
      return sequenceNumber;
    }

    const T& item() const {
      return currentBlock->contents[nextPos];
    }

    void next() {
      sequenceNumber++;
      nextPos++;
      if(nextPos == BlockSize) {
        nextPos = 0;
        currentBlock = currentBlock->next.get();
      }
    }

  private:
    MemoryBlock<T, BlockSize> *currentBlock = nullptr;
    size_t nextPos = -1;
    int64_t sequenceNumber;
  };

  Iterator begin() {
    //--------------------------------------------------------------------------
    // We don't lock popMutex here, you should synchronize the two externally.
    // There's no use case where calling begin() is safe if there are concurrent
    // pops. By not locking here, we'll get a nice warning with TSan if that
    // happens.
    //--------------------------------------------------------------------------
    return Iterator(root.get(), firstBlockNextToPop, frontSequenceNumber);
  }

  bool empty() {
    std::lock_guard<std::mutex> lock(pushMutex);
    std::lock_guard<std::mutex> lock2(popMutex);
    return root->next.get() == nullptr && nextSequenceNumber == frontSequenceNumber;
  }

private:
  //----------------------------------------------------------------------------
  // Remove the root node, and make its child the root.
  //----------------------------------------------------------------------------
  void removeRoot() {
    std::unique_ptr<MemoryBlock<T, BlockSize>> child = std::move(root->next);
    root = std::move(child);
    firstBlockNextToPop = 0;
  }

  //----------------------------------------------------------------------------
  // Allocate new block.
  //----------------------------------------------------------------------------
  void allocateBlock() {
    lastBlock->next = std::unique_ptr<MemoryBlock<T, BlockSize>>(new MemoryBlock<T, BlockSize>());
    lastBlockNextPos = 0;
    lastBlock = lastBlock->next.get();
  }

  std::unique_ptr<MemoryBlock<T, BlockSize>> root;
  MemoryBlock<T, BlockSize> *lastBlock;

  size_t firstBlockNextToPop;
  size_t lastBlockNextPos;

  int64_t nextSequenceNumber;
  int64_t frontSequenceNumber;

  std::mutex pushMutex;
  std::mutex popMutex;
};

}

#endif
