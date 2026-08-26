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

// Worker 池承接网络线程之外的业务和持久化工作，避免 Reactor 被阻塞。
class ThreadPool {
 public:
  using Task = std::function<void()>;

  ThreadPool(std::size_t worker_count, std::size_t queue_capacity);
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  // 队列满时立即失败，由调用方决定重试或触发背压。
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
