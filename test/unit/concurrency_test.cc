/**
 * @file concurrency_test.cc
 * @brief 并发组件单元测试
 * 
 * 本文件包含对消息队列并发组件功能的单元测试，验证以下功能：
 * 1. MPMC（多生产者多消费者）有界队列的行为
 * 2. MPMC队列的多线程一致性（无重复、无丢失）
 * 3. 线程池的任务执行和关闭行为
 * 4. Slab分配器的大小类映射、分配/释放、扩容、线程所有权
 * 5. 对象池的获取/释放、重置、线程所有权
 * 6. 内存池的重置与复用
 * 
 * 测试使用多线程环境验证并发安全性。
 */

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "mq/core/buffer.h"
#include "mq/core/object_pool.h"
#include "mq/core/slab_allocator.h"
#include "mq/core/thread_pool.h"

namespace {

/**
 * @brief 测试有界队列的基本行为（容量、入队、出队、溢出）
 */
void BoundedQueueBehavior() {
  mq::core::MpmcQueue<std::unique_ptr<int>> queue(3);
  assert(queue.capacity() == 4);
  std::unique_ptr<int> value;
  assert(!queue.TryDequeue(&value));
  assert(queue.TryEnqueue(std::make_unique<int>(1)));
  assert(queue.TryEnqueue(std::make_unique<int>(2)));
  assert(queue.TryEnqueue(std::make_unique<int>(3)));
  assert(queue.TryEnqueue(std::make_unique<int>(4)));
  assert(!queue.TryEnqueue(std::make_unique<int>(5)));
  assert(queue.TryDequeue(&value) && *value == 1);
  assert(queue.TryEnqueue(std::make_unique<int>(5)));
  while (queue.TryDequeue(&value)) {
  }
  assert(!queue.TryDequeue(&value));
}

/**
 * @brief 测试MPMC队列的多线程一致性（无重复消费、无数据丢失）
 */
void MpmcConsistency() {
  constexpr std::size_t kProducerCount = 4;
  constexpr std::size_t kConsumerCount = 4;
  constexpr std::size_t kItemsPerProducer = 1000;
  constexpr std::size_t kTotalItems = kProducerCount * kItemsPerProducer;
  mq::core::MpmcQueue<std::size_t> queue(256);
  std::vector<std::atomic<std::size_t>> seen(kTotalItems);
  std::atomic<std::size_t> consumed{0};
  std::atomic<bool> producers_done{false};
  std::vector<std::thread> consumers;
  for (std::size_t index = 0; index < kConsumerCount; ++index) {
    consumers.emplace_back([&] {
      for (;;) {
        std::size_t value = 0;
        if (queue.TryDequeue(&value)) {
          assert(value < kTotalItems);
          assert(seen[value].fetch_add(1, std::memory_order_relaxed) == 0);
          consumed.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        if (producers_done.load(std::memory_order_acquire) &&
            consumed.load(std::memory_order_acquire) == kTotalItems) {
          return;
        }
        std::this_thread::yield();
      }
    });
  }
  std::vector<std::thread> producers;
  for (std::size_t producer = 0; producer < kProducerCount; ++producer) {
    producers.emplace_back([&, producer] {
      for (std::size_t index = 0; index < kItemsPerProducer; ++index) {
        const std::size_t value = producer * kItemsPerProducer + index;
        while (!queue.TryEnqueue(value)) std::this_thread::yield();
      }
    });
  }
  for (auto& producer : producers) producer.join();
  producers_done.store(true, std::memory_order_release);
  for (auto& consumer : consumers) consumer.join();
  assert(consumed.load(std::memory_order_relaxed) == kTotalItems);
  for (const auto& count : seen) assert(count.load(std::memory_order_relaxed) == 1);
}

/**
 * @brief 测试线程池的任务执行和关闭行为
 */
void ThreadPoolExecutesAndDrains() {
  constexpr std::size_t kTaskCount = 128;
  std::atomic<std::size_t> completed{0};
  mq::core::ThreadPool pool(4, 256);
  for (std::size_t index = 0; index < kTaskCount; ++index) {
    assert(pool.Submit([&completed] { completed.fetch_add(1, std::memory_order_relaxed); }));
  }
  pool.Shutdown();
  assert(completed.load(std::memory_order_relaxed) == kTaskCount);
  assert(!pool.Submit([] {}));
}

// ========== SlabAllocator 测试 ==========

/**
 * @brief 测试Slab分配器的大小类索引和字节数映射
 */
void SlabAllocatorSizeClassMapping() {
  // 验证大小类索引计算
  assert(mq::core::SlabAllocator::SizeClassIndex(1) == 0);    // 16
  assert(mq::core::SlabAllocator::SizeClassIndex(16) == 0);   // 16
  assert(mq::core::SlabAllocator::SizeClassIndex(17) == 1);   // 32
  assert(mq::core::SlabAllocator::SizeClassIndex(32) == 1);   // 32
  assert(mq::core::SlabAllocator::SizeClassIndex(33) == 2);   // 64
  assert(mq::core::SlabAllocator::SizeClassIndex(64) == 2);   // 64
  assert(mq::core::SlabAllocator::SizeClassIndex(4096) == 8); // 4096
  // 超过最大值走 malloc
  assert(mq::core::SlabAllocator::SizeClassIndex(4097) == 9);

  // 验证大小类字节数
  assert(mq::core::SlabAllocator::SizeClassBytes(0) == 16);
  assert(mq::core::SlabAllocator::SizeClassBytes(1) == 32);
  assert(mq::core::SlabAllocator::SizeClassBytes(8) == 4096);
}

/**
 * @brief 测试Slab分配器的基本分配和释放功能（LIFO顺序复用）
 */
void SlabAllocatorBasicAllocateDeallocate() {
  mq::core::SlabAllocator allocator(4096);

  // 分配 4 个 32 字节块
  void* p1 = allocator.Allocate(30);
  void* p2 = allocator.Allocate(30);
  void* p3 = allocator.Allocate(30);
  void* p4 = allocator.Allocate(30);
  assert(p1 != nullptr);
  assert(p2 != nullptr);
  assert(p3 != nullptr);
  assert(p4 != nullptr);
  // 所有指针应不同
  assert(p1 != p2 && p1 != p3 && p1 != p4);

  // 归还 p2 和 p4
  allocator.Deallocate(p2, 30);
  allocator.Deallocate(p4, 30);

  // 再次分配，应该复用 p4 和 p2（LIFO 顺序）
  void* p5 = allocator.Allocate(30);
  void* p6 = allocator.Allocate(30);
  assert(p5 == p4);  // LIFO: 最后归还的最先分配
  assert(p6 == p2);
}

/**
 * @brief 测试Slab分配器不同大小类的分配和复用
 */
void SlabAllocatorDifferentSizeClasses() {
  mq::core::SlabAllocator allocator(4096);

  // 不同大小分配到不同桶
  void* small = allocator.Allocate(16);
  void* medium = allocator.Allocate(100);
  void* large = allocator.Allocate(2000);
  assert(small != nullptr);
  assert(medium != nullptr);
  assert(large != nullptr);

  // 归还后复用
  allocator.Deallocate(small, 16);
  allocator.Deallocate(medium, 100);
  allocator.Deallocate(large, 2000);

  void* small2 = allocator.Allocate(16);
  void* medium2 = allocator.Allocate(100);
  void* large2 = allocator.Allocate(2000);
  assert(small2 == small);
  assert(medium2 == medium);
  assert(large2 == large);
}

/**
 * @brief 测试Slab分配器对大对象（>4096字节）使用malloc分配
 */
void SlabAllocatorLargeObjectMalloc() {
  mq::core::SlabAllocator allocator(4096);

  // 超过 4096 字节走 malloc
  void* big = allocator.Allocate(8192);
  assert(big != nullptr);
  // 可以写入验证
  std::memset(big, 0xAB, 8192);
  allocator.Deallocate(big, 8192);
  // 归还后再次分配，malloc 可能复用也可能不复用，只验证不崩溃
  void* big2 = allocator.Allocate(8192);
  assert(big2 != nullptr);
  allocator.Deallocate(big2, 8192);
}

/**
 * @brief 测试Slab分配器的线程所有权（跨线程分配应失败）
 */
void SlabAllocatorThreadOwnership() {
  mq::core::SlabAllocator allocator(4096);
  assert(allocator.IsOwnerThread());

  std::atomic<void*> foreign_result{reinterpret_cast<void*>(1)};
  std::thread foreign_thread([&] {
    foreign_result.store(allocator.Allocate(32));
  });
  foreign_thread.join();
  // 跨线程分配应失败
  assert(foreign_result.load() == nullptr);
}

/**
 * @brief 测试Slab分配器的扩容功能
 */
void SlabAllocatorExpansion() {
  // 极小初始容量，迫使扩容
  mq::core::SlabAllocator allocator(64);

  // 分配大量 32 字节块，触发多次扩容
  std::vector<void*> blocks;
  for (int i = 0; i < 200; ++i) {
    void* block = allocator.Allocate(24);
    assert(block != nullptr);
    blocks.push_back(block);
  }

  // 全部归还
  for (void* block : blocks) {
    allocator.Deallocate(block, 24);
  }

  // 全部再次分配，应从空闲链表复用
  for (int i = 0; i < 200; ++i) {
    void* block = allocator.Allocate(24);
    assert(block != nullptr);
  }
}

/**
 * @brief 测试Slab分配器分配0字节返回nullptr
 */
void SlabAllocatorAllocateZeroReturnsNull() {
  mq::core::SlabAllocator allocator(4096);
  assert(allocator.Allocate(0) == nullptr);
}

/**
 * @brief 测试Slab分配器释放nullptr不崩溃
 */
void SlabAllocatorDeallocateNullNoOp() {
  mq::core::SlabAllocator allocator(4096);
  // 不应崩溃
  allocator.Deallocate(nullptr, 32);
}

// ========== ObjectPool 测试 ==========

/**
 * @brief 测试对象池的获取和释放功能（LIFO顺序复用）
 */
void ObjectPoolAcquireRelease() {
  mq::core::ObjectPool<int> pool(4);

  int* a = pool.Acquire();
  int* b = pool.Acquire();
  int* c = pool.Acquire();
  int* d = pool.Acquire();
  assert(a != nullptr && b != nullptr && c != nullptr && d != nullptr);
  assert(a != b && a != c && a != d);

  // 全部被重置为 0
  assert(*a == 0 && *b == 0 && *c == 0 && *d == 0);

  // 池已空
  assert(pool.Acquire() == nullptr);
  assert(pool.available() == 0);

  // 归还两个
  pool.Release(b);
  pool.Release(d);
  assert(pool.available() == 2);

  // 再次获取，应复用（LIFO）
  int* e = pool.Acquire();
  int* f = pool.Acquire();
  assert(e == d);
  assert(f == b);
  assert(*e == 0 && *f == 0);
}

/**
 * @brief 测试对象池释放时重置对象状态
 */
void ObjectPoolResetOnRelease() {
  mq::core::ObjectPool<int> pool(2);

  int* a = pool.Acquire();
  *a = 42;
  pool.Release(a);

  int* b = pool.Acquire();
  // 归还后重置为 0
  assert(*b == 0);
  assert(b == a);
}

/**
 * @brief 测试对象池的线程所有权（跨线程获取应失败）
 */
void ObjectPoolThreadOwnership() {
  mq::core::ObjectPool<int> pool(4);
  assert(pool.IsOwnerThread());

  std::atomic<int*> foreign_result{reinterpret_cast<int*>(1)};
  std::thread foreign_thread([&] {
    foreign_result.store(pool.Acquire());
  });
  foreign_thread.join();
  assert(foreign_result.load() == nullptr);
}

/**
 * @brief 测试对象池的容量管理
 */
void ObjectPoolCapacity() {
  mq::core::ObjectPool<int> pool(3);
  assert(pool.capacity() == 3);
  assert(pool.available() == 3);
  pool.Acquire();
  assert(pool.available() == 2);
}

// ========== MemoryPool Reset 测试 ==========

/**
 * @brief 测试内存池的重置与复用功能
 */
void MemoryPoolResetReuse() {
  mq::core::MemoryPool pool(256);

  void* p1 = pool.Allocate(64, alignof(std::max_align_t));
  void* p2 = pool.Allocate(64, alignof(std::max_align_t));
  assert(p1 != nullptr && p2 != nullptr);
  assert(pool.used_bytes() >= 128);

  pool.Reset();
  assert(pool.used_bytes() == 0);

  // 重置后应能再次分配
  void* p3 = pool.Allocate(64, alignof(std::max_align_t));
  assert(p3 != nullptr);
  // 重置后从起始位置分配，地址应与首次相同
  assert(p3 == p1);
}

}  // namespace

/**
 * @brief 主函数，运行所有并发组件单元测试
 * @return 0 表示测试成功
 */
int main() {
  BoundedQueueBehavior();
  MpmcConsistency();
  ThreadPoolExecutesAndDrains();

  // SlabAllocator
  SlabAllocatorSizeClassMapping();
  SlabAllocatorBasicAllocateDeallocate();
  SlabAllocatorDifferentSizeClasses();
  SlabAllocatorLargeObjectMalloc();
  SlabAllocatorThreadOwnership();
  SlabAllocatorExpansion();
  SlabAllocatorAllocateZeroReturnsNull();
  SlabAllocatorDeallocateNullNoOp();

  // ObjectPool
  ObjectPoolAcquireRelease();
  ObjectPoolResetOnRelease();
  ObjectPoolThreadOwnership();
  ObjectPoolCapacity();

  // MemoryPool Reset
  MemoryPoolResetReuse();

  return 0;
}
