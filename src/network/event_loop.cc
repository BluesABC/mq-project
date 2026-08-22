#include "mq/network/event_loop.h"

#include <chrono>
#include <utility>

#ifndef _WIN32
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif

namespace mq::network {

EventLoop::EventLoop(std::size_t queue_capacity) : queue_(queue_capacity) {}

EventLoop::~EventLoop() { Stop(); }

bool EventLoop::Start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
#ifndef _WIN32
  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (epoll_fd_ < 0 || wake_fd_ < 0) {
    if (wake_fd_ >= 0) close(wake_fd_);
    if (epoll_fd_ >= 0) close(epoll_fd_);
    started_.store(false, std::memory_order_release);
    return false;
  }
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.fd = wake_fd_;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &event) != 0) {
    close(wake_fd_);
    close(epoll_fd_);
    wake_fd_ = -1;
    epoll_fd_ = -1;
    started_.store(false, std::memory_order_release);
    return false;
  }
#endif
  try {
    thread_ = std::thread(&EventLoop::Run, this);
  } catch (...) {
#ifndef _WIN32
    close(wake_fd_);
    close(epoll_fd_);
    wake_fd_ = -1;
    epoll_fd_ = -1;
#endif
    started_.store(false, std::memory_order_release);
    throw;
  }
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
#ifndef _WIN32
  if (wake_fd_ >= 0) { std::uint64_t value = 1; (void)write(wake_fd_, &value, sizeof(value)); }
#endif
  return true;
}

bool EventLoop::RegisterFd(int fd, std::uint32_t events, FdCallback callback) {
#ifdef _WIN32
  (void)fd; (void)events; (void)callback; return false;
#else
  if (fd < 0 || !callback || epoll_fd_ < 0 || !IsInLoopThread()) return false;
  epoll_event event{}; event.events = events; event.data.fd = fd;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) return false;
  std::lock_guard lock(fd_mutex_); fd_callbacks_[fd] = std::move(callback); return true;
#endif
}

bool EventLoop::ModifyFd(int fd, std::uint32_t events) {
#ifdef _WIN32
  (void)fd; (void)events; return false;
#else
  epoll_event event{}; event.events = events; event.data.fd = fd;
  return epoll_fd_ >= 0 && IsInLoopThread() && epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) == 0;
#endif
}

bool EventLoop::RemoveFd(int fd) {
#ifdef _WIN32
  (void)fd; return false;
#else
  if (epoll_fd_ < 0 || !IsInLoopThread()) return false;
  epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
  std::lock_guard lock(fd_mutex_); fd_callbacks_.erase(fd); return true;
#endif
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
#ifndef _WIN32
  if (wake_fd_ >= 0) close(wake_fd_);
  if (epoll_fd_ >= 0) close(epoll_fd_);
  wake_fd_ = -1; epoll_fd_ = -1;
#endif
}

void EventLoop::Run() {
  try {
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
        // A polling callback must not terminate its owner thread.
      }
    }
#ifndef _WIN32
    epoll_event events[64];
    const int count = epoll_wait(epoll_fd_, events, 64, 5);
    for (int index = 0; index < count; ++index) {
      if (events[index].data.fd == wake_fd_) {
        std::uint64_t value;
        while (read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {}
        continue;
      }
      FdCallback callback;
      { std::lock_guard lock(fd_mutex_); auto it = fd_callbacks_.find(events[index].data.fd); if (it != fd_callbacks_.end()) callback = it->second; }
      if (callback) {
        try {
          callback(events[index].events);
        } catch (...) {
          // Do not allow a network callback to terminate the Reactor thread.
          RemoveFd(events[index].data.fd);
        }
      }
    }
    if (stopping_.load(std::memory_order_acquire) && queued_tasks_.load(std::memory_order_acquire) == 0) return;
#else
    std::unique_lock lock(wait_mutex_);
    wait_condition_.wait_for(lock, std::chrono::milliseconds(5), [this] {
      return stopping_.load(std::memory_order_acquire) ||
             queued_tasks_.load(std::memory_order_acquire) != 0;
    });
    if (stopping_.load(std::memory_order_acquire) &&
        queued_tasks_.load(std::memory_order_acquire) == 0) {
      return;
    }
#endif
  }
  } catch (...) {
    // No exception may escape the Reactor thread entry point.
  }
}

}  // namespace mq::network
