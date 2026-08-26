#pragma once

#include <cstddef>
#include <memory>
#include <thread>

namespace mq::core {

// 线性内存池只允许创建它的线程使用，避免跨线程归还对象造成释放竞态。
class MemoryPool {
 public:
  explicit MemoryPool(std::size_t capacity_bytes);

  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;

  // 分配失败返回 nullptr；池不提供单独释放，生命周期随池统一结束。
  void* Allocate(std::size_t size, std::size_t alignment);
  bool IsOwnerThread() const;
  std::size_t capacity_bytes() const {
    return capacity_bytes_;
  }
  std::size_t used_bytes() const {
    return used_bytes_;
  }

 private:
  const std::thread::id owner_thread_id_;
  const std::size_t capacity_bytes_;
  std::unique_ptr<std::byte[]> storage_;
  std::size_t used_bytes_ = 0;
};

}  // namespace mq::core
