#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace mq::core {

/**
 * @brief 无锁多生产者多消费者队列（Lock-Free MPMC Queue）
 *
 * 基于 Dmitry Vyukov 的 bounded MPMC queue 实现，提供 O(1) 的入队和出队操作。
 *
 * 核心设计：
 * 1. 固定容量：使用环形数组，避免热路径扩容
 * 2. 序号同步：每个槽位有独立的序号，用于区分"可写"和"可读"状态
 * 3. CAS 操作：使用 compare_exchange_weak 实现无锁并发
 *
 * 使用场景：
 * - ThreadPool 的任务队列
 * - EventLoop 的跨线程任务队列
 * - 任何需要无锁并发队列的场景
 *
 * 与其他模块的关系：
 * - ThreadPool 和 EventLoop 都使用本类作为任务队列
 * - 调用方负责处理 TryEnqueue/TryDequeue 的失败情况
 *
 * 注意事项：
 * - 容量必须是 2 的幂（内部自动对齐）
 * - 不是无等待（wait-free）的，但保证无锁（lock-free）
 * - 线程安全性：多线程并发调用安全
 */
template <typename T>
class MpmcQueue {
 public:
  /**
   * @brief 构造函数，创建指定容量的队列
   *
   * 容量会被向上对齐到 2 的幂，以支持位运算优化。
   *
   * @param capacity 期望的队列容量，最小值为 2
   */
  explicit MpmcQueue(std::size_t capacity)
      : capacity_(NormalizeCapacity(capacity)), mask_(capacity_ - 1), cells_(new Cell[capacity_]) {
    // 初始化每个槽位的序号为其索引值
    // 这保证了初始状态下每个槽位都是"可写"的
    for (std::size_t index = 0; index < capacity_; ++index) {
      cells_[index].sequence.store(index, std::memory_order_relaxed);
    }
  }

  /**
   * @brief 析构函数，清空队列并释放内存
   *
   * 会逐个调用元素的析构函数，确保资源正确释放。
   */
  ~MpmcQueue() {
    while (DiscardOne()) {
    }
  }

  // 禁用拷贝，避免浅拷贝问题
  MpmcQueue(const MpmcQueue&) = delete;
  MpmcQueue& operator=(const MpmcQueue&) = delete;

  /**
   * @brief 尝试原地构造元素入队（推荐使用）
   *
   * 相比 TryEnqueue，避免了元素的拷贝/移动构造。
   * 适用于元素构造成本较高的场景。
   *
   * 算法原理：
   * 1. 加载当前入队位置
   * 2. 计算对应的槽位和序号差值
   * 3. 如果差值为 0，表示槽位可写，使用 CAS 原子递增位置
   * 4. 如果差值 < 0，队列已满
   * 5. 如果差值 > 0，有其他线程正在入队，重试
   *
   * @param args 构造元素的参数
   * @return true 入队成功；false 队列已满
   *
   * 内存序说明：
   * - acquire：确保能看到生产者之前的所有写操作
   * - release：确保生产者的写操作对消费者可见
   */
  template <typename... Args>
  bool TryEmplace(Args&&... args) {
    Cell* cell = nullptr;
    std::size_t position = enqueue_position_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &cells_[position & mask_];
      // acquire 保证能看到生产者在该槽位的写操作
      const std::size_t sequence = cell->sequence.load(std::memory_order_acquire);
      const std::intptr_t difference =
          static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(position);
      if (difference == 0) {
        // 槽位可写，尝试 CAS 递增入队位置
        if (enqueue_position_.compare_exchange_weak(
                position, position + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
          break;
        }
      } else if (difference < 0) {
        // 队列已满
        return false;
      } else {
        // 其他线程正在入队，重新加载位置
        position = enqueue_position_.load(std::memory_order_relaxed);
      }
    }
    // 使用 placement new 在槽位中构造元素
    new (&cell->storage) T(std::forward<Args>(args)...);
    // release 保证构造操作对消费者可见
    cell->sequence.store(position + 1, std::memory_order_release);
    return true;
  }

  /**
   * @brief 尝试入队（移动语义）
   *
   * @param value 要入队的元素（会被移动）
   * @return true 入队成功；false 队列已满
   */
  bool TryEnqueue(T value) {
    return TryEmplace(std::move(value));
  }

  /**
   * @brief 尝试出队
   *
   * 算法原理与 TryEmplace 类似，但方向相反：
   * 1. 加载当前出队位置
   * 2. 计算序号差值
   * 3. 如果差值为 0，表示槽位可读，使用 CAS 原子递增位置
   * 4. 如果差值 < 0，队列为空
   * 5. 如果差值 > 0，有其他线程正在出队，重试
   *
   * @param value 输出参数，成功时通过移动语义返回元素
   * @return true 出队成功；false 队列为空
   *
   * 注意：出队后会调用元素的移动构造函数和析构函数
   */
  bool TryDequeue(T* value) {
    if (value == nullptr) return false;
    Cell* cell = nullptr;
    std::size_t position = dequeue_position_.load(std::memory_order_relaxed);
    for (;;) {
      cell = &cells_[position & mask_];
      // acquire 保证能看到生产者的写操作
      const std::size_t sequence = cell->sequence.load(std::memory_order_acquire);
      const std::intptr_t difference =
          static_cast<std::intptr_t>(sequence) - static_cast<std::intptr_t>(position + 1);
      if (difference == 0) {
        // 槽位可读，尝试 CAS 递增出队位置
        if (dequeue_position_.compare_exchange_weak(
                position, position + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
          break;
        }
      } else if (difference < 0) {
        // 队列为空
        return false;
      } else {
        // 其他线程正在出队，重新加载位置
        position = dequeue_position_.load(std::memory_order_relaxed);
      }
    }
    // 从槽位移动元素到输出参数
    T* stored = reinterpret_cast<T*>(&cell->storage);
    *value = std::move(*stored);
    // 手动调用析构函数（因为使用了 placement new）
    stored->~T();
    // 更新序号，允许该槽位被重新使用
    // position + capacity_ 表示该槽位已空闲，可以被生产者写入
    cell->sequence.store(position + capacity_, std::memory_order_release);
    return true;
  }

  /**
   * @brief 获取队列容量
   * @return 队列的最大元素数量
   */
  std::size_t capacity() const {
    return capacity_;
  }

 private:
  /**
   * @brief 槽位结构
   *
   * 每个槽位包含一个序号和一个存储区域。
   * 序号用于区分槽位状态：
   * - sequence == position：槽位可写（生产者使用）
   * - sequence == position + 1：槽位可读（消费者使用）
   * - sequence == position + capacity_：槽位已空闲（可以被生产者重用）
   */
  struct Cell {
    /**
     * @brief 槽位序号
     * 原子变量，用于无锁同步
     */
    std::atomic<std::size_t> sequence;

    /**
     * @brief 存储区域
     * 使用 aligned_storage 避免默认构造，支持任意类型 T
     */
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
  };

  /**
   * @brief 将容量向上对齐到 2 的幂
   *
   * 使用位运算快速计算，时间复杂度 O(log n)。
   * 最小容量为 2，因为 1 会导致 mask 为 0，无法正确计算索引。
   *
   * @param capacity 输入容量
   * @return 对齐后的 2 的幂数
   */
  static std::size_t NormalizeCapacity(std::size_t capacity) {
    if (capacity < 2) return 2;
    --capacity;
    for (std::size_t shift = 1; shift < sizeof(std::size_t) * 8; shift <<= 1) {
      capacity |= capacity >> shift;
    }
    return capacity + 1;
  }

  /**
   * @brief 丢弃一个元素（用于析构时清空队列）
   *
   * @return true 成功丢弃；false 队列为空
   */
  bool DiscardOne() {
    const std::size_t position = dequeue_position_.load(std::memory_order_relaxed);
    Cell* cell = &cells_[position & mask_];
    if (cell->sequence.load(std::memory_order_acquire) != position + 1) return false;
    reinterpret_cast<T*>(&cell->storage)->~T();
    dequeue_position_.store(position + 1, std::memory_order_relaxed);
    cell->sequence.store(position + capacity_, std::memory_order_release);
    return true;
  }

  const std::size_t capacity_;     ///< 队列容量（2 的幂）
  const std::size_t mask_;         ///< 掩码，用于位运算计算索引（capacity_ - 1）
  std::unique_ptr<Cell[]> cells_;  ///< 槽位数组

  /**
   * @brief 入队位置（写指针）
   * alignas(64) 确保缓存行对齐，避免 false sharing
   */
  alignas(64) std::atomic<std::size_t> enqueue_position_{0};

  /**
   * @brief 出队位置（读指针）
   * alignas(64) 确保缓存行对齐，避免 false sharing
   */
  alignas(64) std::atomic<std::size_t> dequeue_position_{0};
};

}  // namespace mq::core