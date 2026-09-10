#pragma once

#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

namespace mq::core {

/**
 * @brief 对象池（预分配 + 空闲链表复用）
 *
 * 预分配固定数量的 T 对象，空闲链表管理。
 * 获取时从链表取，归还时放回链表并重置对象状态。
 *
 * 设计要点：
 * 1. 预分配：构造时一次性分配 capacity 个对象，避免热路径 new/delete
 * 2. 空闲链表：O(1) 获取/归还，无锁（单线程使用）
 * 3. 自动重置：归还时自动调用 T 的默认赋值重置状态
 * 4. 单线程归属：仅允许创建线程使用
 * 5. 超限返回：空闲链表为空时返回 nullptr，触发上层背压
 *
 * 使用场景：
 * - TcpConnection 连接对象池（预分配 max_connections 个）
 * - Message 消息对象复用
 * - 请求/响应对象复用
 *
 * 注意事项：
 * - T 必须可默认构造和拷贝赋值（用于 Reset）
 * - 不是线程安全的，仅允许创建线程使用
 * - Acquire 返回的对象可能保留之前的使用痕迹（通过赋值重置清理）
 *
 * @tparam T 池化的对象类型
 */
template <typename T>
class ObjectPool {
 public:
  /**
   * @brief 构造函数，预分配 capacity 个对象
   * @param capacity 池容量，必须 > 0
   */
  explicit ObjectPool(std::size_t capacity)
      : owner_thread_id_(std::this_thread::get_id()), capacity_(capacity) {
    storage_.reserve(capacity);
    free_list_.reserve(capacity);
    for (std::size_t i = 0; i < capacity; ++i) {
      storage_.push_back(std::make_unique<T>());
      free_list_.push_back(storage_.back().get());
    }
  }

  ObjectPool(const ObjectPool&) = delete;
  ObjectPool& operator=(const ObjectPool&) = delete;

  /**
   * @brief 从池中获取一个对象
   *
   * 从空闲链表头部取出对象并重置为默认状态。
   *
   * @return 对象指针，空闲链表为空时返回 nullptr
   */
  T* Acquire() {
    if (!IsOwnerThread() || free_list_.empty()) return nullptr;
    T* obj = free_list_.back();
    free_list_.pop_back();
    // 重置对象到默认状态，避免残留之前使用痕迹
    *obj = T{};
    return obj;
  }

  /**
   * @brief 归还对象到池中
   *
   * 归还前会重置对象状态。调用方归还后不得再使用该指针。
   *
   * @param obj 要归还的对象指针，必须是由本池 Acquire 返回的
   */
  void Release(T* obj) {
    if (obj == nullptr || !IsOwnerThread()) return;
    *obj = T{};
    free_list_.push_back(obj);
  }

  /**
   * @brief 检查当前线程是否是对象池的所有者
   */
  bool IsOwnerThread() const {
    return owner_thread_id_ == std::this_thread::get_id();
  }

  /// 池的总容量
  std::size_t capacity() const {
    return capacity_;
  }
  /// 当前空闲对象数
  std::size_t available() const {
    return free_list_.size();
  }

 private:
  const std::thread::id owner_thread_id_;
  /// 预分配的对象存储（持有所有权）
  std::vector<std::unique_ptr<T>> storage_;
  /// 空闲链表（存储指向 storage_ 中对象的裸指针）
  std::vector<T*> free_list_;
  std::size_t capacity_;
};

}  // namespace mq::core
