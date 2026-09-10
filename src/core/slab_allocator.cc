// SlabAllocator 实现
// 采用 Slab 分配器思想，按 2 的幂次划分大小类（16B ~ 4096B），
// 每个大小类维护独立的空闲链表与后备内存池，减少锁竞争并提升缓存局部性。
// 大于 4096B 的对象直接委托 malloc，避免内部碎片。

#include "mq/core/slab_allocator.h"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>

namespace mq::core {

// 构造函数：绑定当前线程，初始化各大小类桶的元数据。
// 初始容量取用户参数与 block_size*64 的较大值，避免首次分配即触发扩容。
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

// 返回第 index 个大小类的字节数，采用 2 的幂次设计以对齐 CPU 缓存行。
std::size_t SlabAllocator::SizeClassBytes(std::size_t index) {
  return kMinSizeClass << index;
}

// 将请求大小映射到大小类索引，向上取整到最近的 2 的幂。
// 返回 kNumSizeClasses 表示超出 Slab 管理范围，需走 malloc。
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

// 分配入口：检查线程归属，小对象走 Slab 路径，大对象直接 malloc。
void* SlabAllocator::Allocate(std::size_t size) {
  if (size == 0 || !IsOwnerThread()) return nullptr;

  if (size > kMaxSizeClass) {
    void* ptr = std::malloc(size);
    return ptr;
  }

  const std::size_t index = SizeClassIndex(size);
  return AllocateFromBucket(buckets_[index]);
}

// 释放入口：小对象归还到对应桶的空闲链表，大对象直接 free。
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

// 从桶分配：优先复用空闲块，其次从后备池切割，最后创建新池（倍增策略）。
// 倍增扩容可摊销系统调用开销，同时限制内存碎片。
void* SlabAllocator::AllocateFromBucket(Bucket& bucket) {
  // 优先从空闲链表取
  if (bucket.free_list != nullptr) {
    FreeNode* node = bucket.free_list;
    bucket.free_list = node->next;
    --bucket.free_count;
    return static_cast<void*>(node);
  }

  // 空闲链表为空，尝试从当前后备池分配
  const std::size_t block_size = bucket.block_size;
  if (!bucket.pools.empty()) {
    MemoryPool* current_pool = bucket.pools.back().get();
    void* block = current_pool->Allocate(block_size, alignof(std::max_align_t));
    if (block != nullptr) return block;
  }

  // 后备池耗尽，创建新池（倍增扩容）
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

// 线程归属检查：SlabAllocator 绑定单线程，避免加锁开销。
bool SlabAllocator::IsOwnerThread() const {
  return owner_thread_id_ == std::this_thread::get_id();
}

// 统计所有桶已分配的内存总量，用于监控与容量规划。
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
