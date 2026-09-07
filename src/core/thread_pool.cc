#include "mq/core/thread_pool.h"

#include <stdexcept>
#include <utility>

namespace mq::core {

/**
 * @brief 构造函数，创建工作线程池
 *
 * 构造流程：
 * 1. 初始化任务队列
 * 2. 创建指定数量的工作线程
 * 3. 每个工作线程执行 RunWorker 函数
 *
 * 异常处理：
 * - 如果创建工作线程失败，会停止已创建的线程并重新抛出异常
 *
 * @param worker_count 工作线程数量，必须大于 0
 * @param queue_capacity 任务队列容量，必须是 2 的幂
 *
 * @throws std::invalid_argument 如果 worker_count 为 0
 * @throws std::bad_alloc 如果内存不足
 */
ThreadPool::ThreadPool(std::size_t worker_count, std::size_t queue_capacity)
    : queue_(queue_capacity) {
  // 参数校验
  if (worker_count == 0) throw std::invalid_argument("worker_count must be positive");

  workers_.reserve(worker_count);

  try {
    // 创建工作线程
    for (std::size_t index = 0; index < worker_count; ++index) {
      workers_.emplace_back(&ThreadPool::RunWorker, this);
    }
  } catch (...) {
    // 创建失败，停止已创建的线程
    stopping_.store(true, std::memory_order_release);
    wait_condition_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
    throw;
  }
}

/**
 * @brief 析构函数，优雅关闭线程池
 *
 * 调用 Shutdown 等待所有任务完成。
 */
ThreadPool::~ThreadPool() {
  Shutdown();
}

/**
 * @brief 提交任务到线程池
 *
 * 提交流程：
 * 1. 检查线程池是否正在停止
 * 2. 尝试将任务放入队列
 * 3. 增加待处理任务计数
 * 4. 唤醒一个工作线程
 *
 * @param task 要执行的任务
 * @return true 提交成功；false 队列已满或线程池已停止
 */
bool ThreadPool::Submit(Task task) {
  std::lock_guard lock(wait_mutex_);

  // 检查任务有效性和线程池状态
  if (!task || stopping_.load(std::memory_order_acquire)) return false;

  // 尝试入队
  if (!queue_.TryEnqueue(std::move(task))) return false;

  // 更新计数并唤醒工作线程
  queued_tasks_.fetch_add(1, std::memory_order_release);
  wait_condition_.notify_one();

  return true;
}

/**
 * @brief 优雅关闭线程池
 *
 * 关闭流程：
 * 1. 设置停止标志（使用 CAS 保证只执行一次）
 * 2. 唤醒所有等待的工作线程
 * 3. 等待所有工作线程退出
 *
 * 注意：会等待队列中的所有任务执行完毕。
 */
void ThreadPool::Shutdown() {
  {
    std::lock_guard lock(wait_mutex_);
    // 使用 CAS 保证只执行一次
    bool expected = false;
    if (!stopping_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return;
  }

  // 唤醒所有工作线程
  wait_condition_.notify_all();

  // 等待所有工作线程退出
  for (auto& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
}

/**
 * @brief 工作线程主函数
 *
 * 工作循环：
 * 1. 尝试从队列中取出任务
 * 2. 如果有任务，执行任务
 * 3. 如果没有任务，等待条件变量
 * 4. 收到停止信号且队列为空时退出
 *
 * 异常处理：
 * - 单个任务的异常不会导致工作线程退出
 * - 等待过程中的异常不会导致工作线程退出
 */
void ThreadPool::RunWorker() {
  try {
    for (;;) {
      Task task;

      // 尝试从队列中取出任务
      if (queue_.TryDequeue(&task)) {
        queued_tasks_.fetch_sub(1, std::memory_order_release);

        // 执行任务（捕获异常，防止单个任务崩溃）
        try {
          task();
        } catch (...) {
          // An individual task must not terminate a worker thread.
        }

        continue;
      }

      // 没有任务，等待条件变量
      std::unique_lock lock(wait_mutex_);
      wait_condition_.wait(lock, [this] {
        return stopping_.load(std::memory_order_acquire) ||
               queued_tasks_.load(std::memory_order_acquire) != 0;
      });

      // 检查是否应该退出
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