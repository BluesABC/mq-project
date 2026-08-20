#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

#include "mq/core/buffer.h"
#include "mq/core/thread_pool.h"

namespace {

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
  while (queue.TryDequeue(&value)) {}
  assert(!queue.TryDequeue(&value));
}

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

}  // namespace

int main() {
  BoundedQueueBehavior();
  MpmcConsistency();
  ThreadPoolExecutesAndDrains();
  return 0;
}
