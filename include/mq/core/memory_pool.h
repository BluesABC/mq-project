#pragma once

#include <cstddef>
#include <memory>
#include <thread>

namespace mq::core {

class MemoryPool {
 public:
  explicit MemoryPool(std::size_t capacity_bytes);

  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;

  void* Allocate(std::size_t size, std::size_t alignment);
  bool IsOwnerThread() const;
  std::size_t capacity_bytes() const { return capacity_bytes_; }
  std::size_t used_bytes() const { return used_bytes_; }

 private:
  const std::thread::id owner_thread_id_;
  const std::size_t capacity_bytes_;
  std::unique_ptr<std::byte[]> storage_;
  std::size_t used_bytes_ = 0;
};

}  // namespace mq::core
