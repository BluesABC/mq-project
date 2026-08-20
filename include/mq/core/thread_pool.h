#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "mq/core/buffer.h"

namespace mq::core {

class ThreadPool {
 public:
  using Task = std::function<void()>;

  ThreadPool(std::size_t worker_count, std::size_t queue_capacity);
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  bool Submit(Task task);
  void Shutdown();

 private:
  void RunWorker();

  MpmcQueue<Task> queue_;
  std::atomic<std::size_t> queued_tasks_{0};
  std::atomic<bool> stopping_{false};
  std::mutex wait_mutex_;
  std::condition_variable wait_condition_;
  std::vector<std::thread> workers_;
};

}  // namespace mq::core
