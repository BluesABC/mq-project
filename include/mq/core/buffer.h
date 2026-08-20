#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace mq::core {

// Fixed capacity avoids hot-path growth; sequence values distinguish slot state.
template <typename T>
class MpmcQueue {
 public:
  explicit MpmcQueue(std::size_t capacity)
      : capacity_(NormalizeCapacity(capacity)), mask_(capacity_ - 1), cells_(new Cell[capacity_]) {
    for (std::size_t index = 0; index < capacity_; ++index) {
      cells_[index].sequence.store(index, std::memory_order_relaxed);
    }
  }

  ~MpmcQueue() {
    while (DiscardOne()) {}
  }

  MpmcQueue(const MpmcQueue&) = delete;
  MpmcQueue& operator=(const MpmcQueue&) = delete;

  template <typename... Args>
  bool TryEmplace(Args&&... args) {
    Cell* cell = nullptr;
    std::size_t position = enqueue_position_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &cells_[position & mask_];
      const std::size_t sequence = cell->sequence.load(std::memory_order_acquire);
      const std::intptr_t difference = static_cast<std::intptr_t>(sequence) -
                                       static_cast<std::intptr_t>(position);
      if (difference == 0) {
        if (enqueue_position_.compare_exchange_weak(position, position + 1,
                                                    std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
          break;
        }
      } else if (difference < 0) {
        return false;
      } else {
        position = enqueue_position_.load(std::memory_order_relaxed);
      }
    }
    new (&cell->storage) T(std::forward<Args>(args)...);
    cell->sequence.store(position + 1, std::memory_order_release);
    return true;
  }

  bool TryEnqueue(T value) { return TryEmplace(std::move(value)); }

  bool TryDequeue(T* value) {
    if (value == nullptr) return false;
    Cell* cell = nullptr;
    std::size_t position = dequeue_position_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &cells_[position & mask_];
      const std::size_t sequence = cell->sequence.load(std::memory_order_acquire);
      const std::intptr_t difference = static_cast<std::intptr_t>(sequence) -
                                       static_cast<std::intptr_t>(position + 1);
      if (difference == 0) {
        if (dequeue_position_.compare_exchange_weak(position, position + 1,
                                                    std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
          break;
        }
      } else if (difference < 0) {
        return false;
      } else {
        position = dequeue_position_.load(std::memory_order_relaxed);
      }
    }
    T* stored = reinterpret_cast<T*>(&cell->storage);
    *value = std::move(*stored);
    stored->~T();
    cell->sequence.store(position + capacity_, std::memory_order_release);
    return true;
  }

  std::size_t capacity() const { return capacity_; }

 private:
  struct Cell {
    std::atomic<std::size_t> sequence;
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
  };

  static std::size_t NormalizeCapacity(std::size_t capacity) {
    if (capacity < 2) return 2;
    --capacity;
    for (std::size_t shift = 1; shift < sizeof(std::size_t) * 8; shift <<= 1) {
      capacity |= capacity >> shift;
    }
    return capacity + 1;
  }

  bool DiscardOne() {
    const std::size_t position = dequeue_position_.load(std::memory_order_relaxed);
    Cell* cell = &cells_[position & mask_];
    if (cell->sequence.load(std::memory_order_acquire) != position + 1) return false;
    reinterpret_cast<T*>(&cell->storage)->~T();
    dequeue_position_.store(position + 1, std::memory_order_relaxed);
    cell->sequence.store(position + capacity_, std::memory_order_release);
    return true;
  }

  const std::size_t capacity_;
  const std::size_t mask_;
  std::unique_ptr<Cell[]> cells_;
  alignas(64) std::atomic<std::size_t> enqueue_position_{0};
  alignas(64) std::atomic<std::size_t> dequeue_position_{0};
};

}  // namespace mq::core
