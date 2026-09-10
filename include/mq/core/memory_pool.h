#pragma once

#include <cstddef>
#include <memory>
#include <thread>

namespace mq::core {

/**
 * @brief 线性内存池
 *
 * 线性分配，O(1) 复杂度；Reset() 后池可复用，避免重新分配大块内存。
 *
 * 设计要点：
 * 1. 线性分配：每次分配只移动指针，O(1) 复杂度，无碎片
 * 2. 单线程归属：只允许创建它的线程使用，避免跨线程归还造成释放竞态
 * 3. 批量释放：内存生命周期随池统一结束，无需单独释放
 *
 * 使用场景：
 * - 消息缓冲区分配
 * - 连接对象分配
 * - 协议解码缓冲区
 *
 * 与 SlabAllocator 的关系：每个大小类的桶内部使用本类作为后备线性存储，
 * 桶耗尽时扩容创建新池。
 *
 * 注意事项：
 * - 不是线程安全的，仅允许创建线程使用
 * - 分配失败时返回 nullptr，调用方需处理（Slab 场景下触发扩容）
 * - 不支持单独释放，适合生命周期统一管理的场景
 */
class MemoryPool {
 public:
  /**
   * @brief 构造函数，预分配指定容量的内存块
   * @param capacity_bytes 内存池容量（字节），必须大于 0
   */
  explicit MemoryPool(std::size_t capacity_bytes);

  // 禁用拷贝，防止多个实例共享同一块内存
  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;

  /**
   * @brief 分配指定大小和对齐方式的内存块。
   * 线性递增，空间不足返回 nullptr（不会扩展）。
   *
   * @param size 需要分配的字节数
   * @param alignment 内存对齐要求（字节），必须是 2 的幂
   * @return 分配的内存指针，失败返回 nullptr
   */
  void* Allocate(std::size_t size, std::size_t alignment);

  /**
   * @brief 重置池的写指针，使已分配内存可被覆盖。
   *
   * 仅归零 used_bytes_，不释放底层内存。
   * 调用方必须保证所有通过本池分配的对象已经不再被使用。
   *
   * 使用场景：
   * - SlabAllocator 桶内所有块均归还到空闲链表后，可重置底层池
   * - 请求处理完成后批量重置 Arena
   */
  void Reset();

  /**
   * @brief 检查当前线程是否是内存池的所有者
   */
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
