#include "mq/core/thread_pool.h"

#include <stdexcept>
#include <utility>

namespace mq::core {

ThreadPool::ThreadPool(std::size_t worker_count, std::size_t queue_capacity) : queue_(queue_capacity) {
  if (worker_count == 0) throw std::invalid_argument("worker_count must be positive");
  workers_.reserve(worker_count);
  try {
    for (std::size_t index = 0; index < worker_count; ++index) {
      workers_.emplace_back(&ThreadPool::RunWorker, this);
    }
  } catch (...) {
    stopping_.store(true, std::memory_order_release);
    wait_condition_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
    throw;
  }
}

ThreadPool::~ThreadPool() { Shutdown(); }

bool ThreadPool::Submit(Task task) {
  std::lock_guard lock(wait_mutex_);
  if (!task || stopping_.load(std::memory_order_acquire)) return false;
  if (!queue_.TryEnqueue(std::move(task))) return false;
  queued_tasks_.fetch_add(1, std::memory_order_release);
  wait_condition_.notify_one();
  return true;
}

void ThreadPool::Shutdown() {
  {
    std::lock_guard lock(wait_mutex_);
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return;
  }
  wait_condition_.notify_all();
  for (auto& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
}

void ThreadPool::RunWorker() {
  try {
  for (;;) {
    Task task;
    if (queue_.TryDequeue(&task)) {
      queued_tasks_.fetch_sub(1, std::memory_order_release);
      try {
        task();
      } catch (...) {
        // An individual task must not terminate a worker thread.
      }
      continue;
    }
    std::unique_lock lock(wait_mutex_);
    wait_condition_.wait(lock, [this] {
      return stopping_.load(std::memory_order_acquire) ||
             queued_tasks_.load(std::memory_order_acquire) != 0;
    });
    if (stopping_.load(std::memory_order_acquire) &&
        queued_tasks_.load(std::memory_order_acquire) == 0) {
      return;
    }
  }
  } catch (...) {
    // A worker must never terminate the process because of a task or wait error.
  }
}

}  // namespace mq::core
