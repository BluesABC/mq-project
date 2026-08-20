#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <future>

#include "mq/network/event_loop.h"

namespace {

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

int main() {
  ExecutesOnOwnerThreadAndDrains();
  IdleCallbackRunsOnOwnerThread();
  return 0;
}
