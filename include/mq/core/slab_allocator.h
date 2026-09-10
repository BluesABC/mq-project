#pragma once

#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

#include "mq/core/memory_pool.h"

namespace mq::core {

/**
 * @brief Slab 分桶分配器
 *
 * 按大小类分桶（16/32/64/128/256/512/1024/2048/4096 字节），
 * 每个桶内部使用侵入式空闲链表 + 线性后备池实现高频分配/释放复用。
 *
 * 设计要点：
 * 1. 大小类分桶：向上取整到最近大小类，避免碎片
 * 2. 侵入式空闲链表：释放的块直接链入空闲链表，下次分配时优先复用
 * 3. 倍增扩容：后备池耗尽时创建新池，容量翻倍
 * 4. 大对象直通：超过 4096B 的分配走 malloc/free
 * 5. 单线程归属：仅允许创建线程使用，释放也必须在同一线程
 *
 * 使用场景：
 * - Worker 线程内的消息缓冲分配/释放
 * - 协议解码缓冲的频繁分配/释放
 * - 连接对象内部临时缓冲
 *
 * 与其他模块的关系：
 * - 每个 Worker 线程可持有独立 SlabAllocator，避免跨线程竞争
 * - 内部使用 MemoryPool 作为后备线性存储
 * - 超限时返回 nullptr，由上层触发背压
 *
 * 线程安全性：非线程安全，仅允许创建线程使用
 */
class SlabAllocator {
 public:
  /// 大小类数量：16, 32, 64, 128, 256, 512, 1024, 2048, 4096
  static constexpr std::size_t kNumSizeClasses = 9;
  /// 最小大小类（字节）
  static constexpr std::size_t kMinSizeClass = 16;
  /// 最大大小类（字节），超过此值走 malloc/free
  static constexpr std::size_t kMaxSizeClass = 4096;

  /**
   * @brief 构造函数
   * @param initial_capacity_per_class 每个大小类的初始后备池容量（字节），默认 256KB
   */
  explicit SlabAllocator(std::size_t initial_capacity_per_class = 256 * 1024);

  ~SlabAllocator();

  SlabAllocator(const SlabAllocator&) = delete;
  SlabAllocator& operator=(const SlabAllocator&) = delete;

  /**
   * @brief 分配 size 字节内存
   *
   * 内部将 size 向上取整到最近大小类。超过 kMaxSizeClass 时走 malloc。
   *
   * @param size 请求字节数，必须 > 0
   * @return 分配的内存指针，失败返回 nullptr
   */
  void* Allocate(std::size_t size);

  /**
   * @brief 归还内存到对应大小类的空闲链表
   *
   * 调用方必须保证：
   * - ptr 是由本分配器的 Allocate 返回的
   * - size 与 Allocate 时的 size 一致
   * - 归还后不再使用 ptr
   * - 在创建线程内调用
   *
   * @param ptr 要归还的内存指针，不能为 nullptr
   * @param size 原始分配大小
   */
  void Deallocate(void* ptr, std::size_t size);

  /**
   * @brief 检查当前线程是否是分配器的所有者
   */
  bool IsOwnerThread() const;

  /**
   * @brief 获取所有桶已分配的总字节数（后备池容量之和）
   */
  std::size_t total_allocated_bytes() const {
    return total_allocated_;
  }

  /**
   * @brief 获取当前从后备池中实际使用的字节数
   */
  std::size_t total_used_bytes() const;

  /**
   * @brief 获取指定大小类索引对应的块大小
   */
  static std::size_t SizeClassBytes(std::size_t index);

  /**
   * @brief 根据 size 查找对应的大小类索引
   * @return 大小类索引，若 size > kMaxSizeClass 则返回 kNumSizeClasses
   */
  static std::size_t SizeClassIndex(std::size_t size);

 private:
  /// 侵入式空闲链表节点：使用空闲块的前 sizeof(void*) 字节存储 next 指针
  struct FreeNode {
    FreeNode* next;
  };

  /**
   * @brief 单个大小类的桶
   */
  struct Bucket {
    /// 侵入式空闲链表头指针
    FreeNode* free_list = nullptr;
    /// 后备线性池列表（扩容时追加）
    std::vector<std::unique_ptr<MemoryPool>> pools;
    /// 本桶的块大小（字节）
    std::size_t block_size = 0;
    /// 下一个新池的容量（字节），首次为 initial_capacity，之后倍增
    std::size_t next_pool_capacity = 0;
    /// 空闲链表中当前可用的块数
    std::size_t free_count = 0;
  };

  /**
   * @brief 从指定桶分配一个块
   *
   * 优先从空闲链表取，链表为空时从后备池切割新块。
   * 后备池也耗尽时触发扩容（创建倍增容量的新池）。
   *
   * @param bucket 目标桶
   * @return 分配的内存指针，失败返回 nullptr
   */
  void* AllocateFromBucket(Bucket& bucket);

  const std::thread::id owner_thread_id_;
  Bucket buckets_[kNumSizeClasses];
  std::size_t total_allocated_ = 0;
};

}  // namespace mq::core
