#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <thread>

#include "mq/core/buffer.h"

namespace mq::network {

class EventLoop {
 public:
  using Task = std::function<void()>;

  explicit EventLoop(std::size_t queue_capacity);
  ~EventLoop();

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  bool Start();
  bool SetIdleCallback(Task callback);
  bool QueueInLoop(Task task);
  bool IsInLoopThread() const;
  void Stop();

 private:
  void Run();

  core::MpmcQueue<Task> queue_;
  std::atomic<std::size_t> queued_tasks_{0};
  std::atomic<bool> started_{false};
  std::atomic<bool> stopping_{false};
  mutable std::mutex wait_mutex_;
  std::condition_variable wait_condition_;
  Task idle_callback_;
  mutable std::mutex owner_mutex_;
  std::thread::id owner_thread_id_;
  std::thread thread_;
};

}  // namespace mq::network
