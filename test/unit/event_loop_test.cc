/**
 * @file event_loop_test.cc
 * @brief 事件循环(EventLoop)单元测试
 * 
 * 本文件包含对事件循环功能的单元测试，验证以下功能：
 * 1. 任务在所有者线程上执行
 * 2. 任务队列的排空(Drain)行为
 * 3. 空闲回调(IdleCallback)在所有者线程上执行
 * 4. 事件循环的启动和停止行为
 * 
 * 测试使用多线程环境验证线程安全性。
 */

#include "mq/network/event_loop.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <future>

namespace {

/**
 * @brief 测试任务在所有者线程上执行并正确排空
 */
void ExecutesOnOwnerThreadAndDrains() {
  constexpr std::size_t kTaskCount = 64;
  mq::network::EventLoop loop(128);
  assert(loop.Start());
  assert(!loop.Start());
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> wrong_thread{false};
  for (std::size_t index = 0; index < kTaskCount; ++index) {
    assert(loop.QueueInLoop([&] {
      if (!loop.IsInLoopThread()) wrong_thread.store(true, std::memory_order_relaxed);
      completed.fetch_add(1, std::memory_order_relaxed);
    }));
  }
  loop.Stop();
  assert(completed.load(std::memory_order_relaxed) == kTaskCount);
  assert(!wrong_thread.load(std::memory_order_relaxed));
  assert(!loop.QueueInLoop([] {}));
}

/**
 * @brief 测试空闲回调在所有者线程上执行
 */
void IdleCallbackRunsOnOwnerThread() {
  mq::network::EventLoop loop(8);
  std::atomic<std::size_t> calls{0};
  std::promise<void> callback_called;
  const auto callback_ready = callback_called.get_future();
  assert(loop.SetIdleCallback([&] {
    assert(loop.IsInLoopThread());
    if (calls.fetch_add(1, std::memory_order_relaxed) == 0) callback_called.set_value();
  }));
  assert(loop.Start());
  assert(callback_ready.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  loop.Stop();
  assert(calls.load(std::memory_order_relaxed) > 0);
}

}  // namespace

/**
 * @brief 主函数，运行所有事件循环单元测试
 * @return 0 表示测试成功
 */
int main() {
  ExecutesOnOwnerThreadAndDrains();
  IdleCallbackRunsOnOwnerThread();
  return 0;
}
