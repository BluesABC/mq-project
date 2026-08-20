#include "mq/network/event_loop.h"

#include <chrono>
#include <utility>

namespace mq::network {

EventLoop::EventLoop(std::size_t queue_capacity) : queue_(queue_capacity) {}

EventLoop::~EventLoop() { Stop(); }

bool EventLoop::Start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
  thread_ = std::thread(&EventLoop::Run, this);
  return true;
}

bool EventLoop::SetIdleCallback(Task callback) {
  std::lock_guard lock(wait_mutex_);
  if (started_.load(std::memory_order_acquire) || !callback) return false;
  idle_callback_ = std::move(callback);
  return true;
}

bool EventLoop::QueueInLoop(Task task) {
  std::lock_guard lock(wait_mutex_);
  if (!task || !started_.load(std::memory_order_acquire) ||
      stopping_.load(std::memory_order_acquire)) {
    return false;
  }
  if (!queue_.TryEnqueue(std::move(task))) return false;
  queued_tasks_.fetch_add(1, std::memory_order_release);
  wait_condition_.notify_one();
  return true;
}

bool EventLoop::IsInLoopThread() const {
  std::lock_guard lock(owner_mutex_);
  return owner_thread_id_ == std::this_thread::get_id();
}

void EventLoop::Stop() {
  if (!started_.load(std::memory_order_acquire)) return;
  {
    std::lock_guard lock(wait_mutex_);
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return;
  }
  wait_condition_.notify_all();
  if (thread_.joinable()) thread_.join();
}

void EventLoop::Run() {
  {
    std::lock_guard lock(owner_mutex_);
    owner_thread_id_ = std::this_thread::get_id();
  }
  for (;;) {
    Task task;
    if (queue_.TryDequeue(&task)) {
      queued_tasks_.fetch_sub(1, std::memory_order_release);
      try {
        task();
      } catch (...) {
        // A callback failure must not terminate the connection owner thread.
      }
      continue;
    }
    if (idle_callback_) {
      try {
        idle_callback_();
      } catch (...) {
        // The polling callback must not terminate its owning loop.
      }
    }
    std::unique_lock lock(wait_mutex_);
    wait_condition_.wait_for(lock, std::chrono::milliseconds(5), [this] {
      return stopping_.load(std::memory_order_acquire) ||
             queued_tasks_.load(std::memory_order_acquire) != 0;
    });
    if (stopping_.load(std::memory_order_acquire) &&
        queued_tasks_.load(std::memory_order_acquire) == 0) {
      return;
    }
  }
}

}  // namespace mq::network
