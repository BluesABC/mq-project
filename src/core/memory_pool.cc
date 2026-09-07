#include "mq/core/memory_pool.h"

#include <limits>
#include <stdexcept>

namespace mq::core {

/**
 * @brief 构造函数，预分配指定容量的内存块
 *
 * @param capacity_bytes 内存池容量（字节），必须大于 0
 *
 * @throws std::invalid_argument 如果 capacity_bytes 为 0
 */
MemoryPool::MemoryPool(std::size_t capacity_bytes)
    : owner_thread_id_(std::this_thread::get_id()), capacity_bytes_(capacity_bytes) {
  // 容量必须大于 0
  if (capacity_bytes == 0) throw std::invalid_argument("capacity_bytes must be positive");

  // 预分配内存
  storage_ = std::make_unique<std::byte[]>(capacity_bytes);
}

/**
 * @brief 分配指定大小和对齐方式的内存块
 *
 * 分配策略：线性递增
 * 1. 计算对齐所需的填充字节
 * 2. 检查剩余空间是否足够
 * 3. 移动写指针，返回分配的内存
 *
 * 对齐计算：
 * - 使用位运算快速计算对齐偏移
 * - 前提：alignment 必须是 2 的幂
 *
 * @param size 需要分配的字节数
 * @param alignment 内存对齐要求（字节），必须是 2 的幂
 * @return 分配的内存指针，失败返回 nullptr
 *
 * 失败条件：
 * - 不是所有者线程
 * - size 为 0
 * - alignment 为 0 或不是 2 的幂
 * - 剩余空间不足
 */
void* MemoryPool::Allocate(std::size_t size, std::size_t alignment) {
  // 参数校验
  if (!IsOwnerThread() || size == 0 || alignment == 0 || (alignment & (alignment - 1)) != 0) {
    return nullptr;
  }

  // 计算对齐所需的填充字节
  // remainder 是当前偏移量对 alignment 取模的结果
  const std::size_t remainder = used_bytes_ & (alignment - 1);
  const std::size_t padding = remainder == 0 ? 0 : alignment - remainder;

  // 检查剩余空间是否足够（填充 + 请求大小）
  if (padding > capacity_bytes_ - used_bytes_ || size > capacity_bytes_ - used_bytes_ - padding) {
    return nullptr;
  }

  // 计算分配的内存地址
  std::byte* memory = storage_.get() + used_bytes_ + padding;

  // 移动写指针
  used_bytes_ += padding + size;

  return memory;
}

/**
 * @brief 检查当前线程是否是内存池的所有者
 *
 * 用于调试和断言，防止跨线程使用。
 *
 * @return true 当前线程是所有者；false 不是所有者
 */
bool MemoryPool::IsOwnerThread() const {
  return owner_thread_id_ == std::this_thread::get_id();
}

}  // namespace mq::core