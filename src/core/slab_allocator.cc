/**
 * @file slab_allocator.cc
 * @brief Slab 分配器实现
 *
 * 采用 Slab 分配器思想，按 2 的幂次划分大小类（16B ~ 4096B），
 * 每个大小类维护独立的空闲链表与后备内存池，减少锁竞争并提升缓存局部性。
 * 大于 4096B 的对象直接委托 malloc，避免内部碎片。
 */

#include "mq/core/slab_allocator.h"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>

namespace mq::core {

/**
 * @brief 构造函数，绑定当前线程并初始化各大小类桶
 * @param initial_capacity_per_class 每个大小类的初始内存池容量
 *
 * 初始容量取用户参数与 block_size*64 的较大值，避免首次分配即触发扩容。
 */
SlabAllocator::SlabAllocator(std::size_t initial_capacity_per_class)
    : owner_thread_id_(std::this_thread::get_id()) {
  if (initial_capacity_per_class == 0) {
    throw std::invalid_argument("initial_capacity_per_class must be positive");
  }
  for (std::size_t index = 0; index < kNumSizeClasses; ++index) {
    const std::size_t block_size = SizeClassBytes(index);
    buckets_[index].block_size = block_size;
    buckets_[index].next_pool_capacity =
        std::max(initial_capacity_per_class, block_size * 64);
  }
}

SlabAllocator::~SlabAllocator() = default;

/**
 * @brief 返回第 index 个大小类的字节数
 * @param index 大小类索引
 * @return 对应大小类的字节数
 *
 * 采用 2 的幂次设计以对齐 CPU 缓存行。
 */
std::size_t SlabAllocator::SizeClassBytes(std::size_t index) {
  return kMinSizeClass << index;
}

/**
 * @brief 将请求大小映射到大小类索引
 * @param size 请求分配的字节数
 * @return 大小类索引，返回 kNumSizeClasses 表示超出 Slab 管理范围
 *
 * 向上取整到最近的 2 的幂，超出范围需走 malloc。
 */
std::size_t SlabAllocator::SizeClassIndex(std::size_t size) {
  if (size == 0) return 0;
  if (size > kMaxSizeClass) return kNumSizeClasses;
  std::size_t aligned = kMinSizeClass;
  while (aligned < size) aligned <<= 1;
  std::size_t index = 0;
  std::size_t temp = aligned / kMinSizeClass;
  while (temp > 1) {
    temp >>= 1;
    ++index;
  }
  return index;
}

/**
 * @brief 分配内存
 * @param size 请求分配的字节数
 * @return 分配成功返回指针，失败返回 nullptr
 *
 * 检查线程归属后，小对象走 Slab 路径，大对象直接 malloc。
 */
void* SlabAllocator::Allocate(std::size_t size) {
  if (size == 0 || !IsOwnerThread()) return nullptr;

  if (size > kMaxSizeClass) {
    void* ptr = std::malloc(size);
    return ptr;
  }

  const std::size_t index = SizeClassIndex(size);
  return AllocateFromBucket(buckets_[index]);
}

/**
 * @brief 释放内存
 * @param ptr 待释放的指针
 * @param size 之前分配的字节数
 *
 * 小对象归还到对应桶的空闲链表，大对象直接 free。
 */
void SlabAllocator::Deallocate(void* ptr, std::size_t size) {
  if (ptr == nullptr || size == 0 || !IsOwnerThread()) return;

  if (size > kMaxSizeClass) {
    std::free(ptr);
    return;
  }

  const std::size_t index = SizeClassIndex(size);
  Bucket& bucket = buckets_[index];

  FreeNode* node = static_cast<FreeNode*>(ptr);
  node->next = bucket.free_list;
  bucket.free_list = node;
  ++bucket.free_count;
}

/**
 * @brief 从指定桶分配内存
 * @param bucket 目标大小类桶
 * @return 分配成功返回指针，失败返回 nullptr
 *
 * 优先复用空闲块，其次从后备池切割，最后创建新池（倍增策略）。
 * 倍增扩容可摊销系统调用开销，同时限制内存碎片。
 */
void* SlabAllocator::AllocateFromBucket(Bucket& bucket) {
  if (bucket.free_list != nullptr) {
    FreeNode* node = bucket.free_list;
    bucket.free_list = node->next;
    --bucket.free_count;
    return static_cast<void*>(node);
  }

  const std::size_t block_size = bucket.block_size;
  if (!bucket.pools.empty()) {
    MemoryPool* current_pool = bucket.pools.back().get();
    void* block = current_pool->Allocate(block_size, alignof(std::max_align_t));
    if (block != nullptr) return block;
  }

  const std::size_t new_capacity = bucket.next_pool_capacity;
  auto new_pool = std::make_unique<MemoryPool>(new_capacity);
  void* block = new_pool->Allocate(block_size, alignof(std::max_align_t));
  if (block == nullptr) {
    new_pool = std::make_unique<MemoryPool>(block_size * 64);
    block = new_pool->Allocate(block_size, alignof(std::max_align_t));
    if (block == nullptr) return nullptr;
  }

  bucket.pools.push_back(std::move(new_pool));
  total_allocated_ += bucket.pools.back()->capacity_bytes();
  bucket.next_pool_capacity = new_capacity * 2;

  return block;
}

/**
 * @brief 检查当前线程是否为所有者线程
 * @return 如果是所有者线程返回 true
 *
 * SlabAllocator 绑定单线程，避免加锁开销。
 */
bool SlabAllocator::IsOwnerThread() const {
  return owner_thread_id_ == std::this_thread::get_id();
}

/**
 * @brief 统计所有桶已分配的内存总量
 * @return 已分配的总字节数
 *
 * 用于监控与容量规划。
 */
std::size_t SlabAllocator::total_used_bytes() const {
  std::size_t total = 0;
  for (std::size_t index = 0; index < kNumSizeClasses; ++index) {
    for (const auto& pool : buckets_[index].pools) {
      total += pool->used_bytes();
    }
  }
  return total;
}

}  // namespace mq::core
