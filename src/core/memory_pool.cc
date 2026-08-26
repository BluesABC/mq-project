#include "mq/core/memory_pool.h"

#include <limits>
#include <stdexcept>

namespace mq::core {

MemoryPool::MemoryPool(std::size_t capacity_bytes)
    : owner_thread_id_(std::this_thread::get_id()), capacity_bytes_(capacity_bytes) {
  if (capacity_bytes == 0) throw std::invalid_argument("capacity_bytes must be positive");
  storage_ = std::make_unique<std::byte[]>(capacity_bytes);
}

void* MemoryPool::Allocate(std::size_t size, std::size_t alignment) {
  if (!IsOwnerThread() || size == 0 || alignment == 0 || (alignment & (alignment - 1)) != 0) {
    return nullptr;
  }
  const std::size_t remainder = used_bytes_ & (alignment - 1);
  const std::size_t padding = remainder == 0 ? 0 : alignment - remainder;
  if (padding > capacity_bytes_ - used_bytes_ || size > capacity_bytes_ - used_bytes_ - padding) {
    return nullptr;
  }
  std::byte* memory = storage_.get() + used_bytes_ + padding;
  used_bytes_ += padding + size;
  return memory;
}

bool MemoryPool::IsOwnerThread() const {
  return owner_thread_id_ == std::this_thread::get_id();
}

}  // namespace mq::core
