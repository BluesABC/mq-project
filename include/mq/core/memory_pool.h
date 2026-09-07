#pragma once

#include <cstddef>
#include <memory>
#include <thread>

namespace mq::core {

/**
 * @brief 线性内存池
 *
 * 为热路径分配固定大小的内存块，避免频繁的 new/malloc 带来的性能开销。
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
 * 与其他模块的关系：
 * - Network 层的 Buffer 和 TcpConnection 通过本类分配内存
 * - StorageEngine 的读写缓冲也使用本类
 *
 * 注意事项：
 * - 不是线程安全的，只允许创建线程使用
 * - 分配失败时返回 nullptr，调用方需处理
 * - 不支持单独释放，适合生命周期统一管理的场景
 */
class MemoryPool {
 public:
  /**
   * @brief 构造函数，预分配指定容量的内存块
   *
   * @param capacity_bytes 内存池容量（字节），必须大于 0
   */
  explicit MemoryPool(std::size_t capacity_bytes);

  // 禁用拷贝，防止多个实例共享同一块内存
  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;

  /**
   * @brief 分配指定大小和对齐方式的内存块
   *
   * 分配策略：线性递增，每次分配只移动指针。
   * 如果剩余空间不足，返回 nullptr（不会扩展）。
   *
   * @param size 需要分配的字节数
   * @param alignment 内存对齐要求（字节），必须是 2 的幂
   * @return 分配的内存指针，失败返回 nullptr
   *
   * 使用示例：
   * @code
   * void* ptr = pool.Allocate(1024, 8);
   * if (ptr) {
   *   // 使用内存...
   * }
   * @endcode
   */
  void* Allocate(std::size_t size, std::size_t alignment);

  /**
   * @brief 检查当前线程是否是内存池的所有者
   *
   * 用于调试和断言，防止跨线程使用。
   *
   * @return true 当前线程是所有者；false 不是所有者
   */
  bool IsOwnerThread() const;

  /**
   * @brief 获取内存池总容量
   * @return 容量（字节）
   */
  std::size_t capacity_bytes() const {
    return capacity_bytes_;
  }

  /**
   * @brief 获取已使用的字节数
   * @return 已使用（字节）
   */
  std::size_t used_bytes() const {
    return used_bytes_;
  }

 private:
  /**
   * @brief 所有者线程 ID
   *
   * 用于 IsOwnerThread() 检查，确保只在创建线程使用。
   * 使用 thread::id 而非 thread*，因为 thread 对象可能提前销毁。
   */
  const std::thread::id owner_thread_id_;

  /**
   * @brief 内存池总容量（字节）
   * 预分配后固定不变，避免动态扩展。
   */
  const std::size_t capacity_bytes_;

  /**
   * @brief 内存存储区域
   * 使用 unique_ptr 管理生命周期，析构时自动释放。
   * 分配对齐为 alignof(std::max_align_t)，满足大多数类型的对齐要求。
   */
  std::unique_ptr<std::byte[]> storage_;

  /**
   * @brief 已使用字节数（写指针位置）
   * 每次分配后递增，归零表示池被重置。
   */
  std::size_t used_bytes_ = 0;
};

}  // namespace mq::core