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

/**
 * @brief 工作线程池
 *
 * 承接网络线程之外的业务和持久化工作，避免 Reactor 线程被阻塞。
 *
 * 在整体架构中的角色：
 * - 网络层（Reactor）只负责收发数据，不处理业务逻辑
 * - 业务处理（如消息存储、偏移量提交）由本类的工作线程执行
 * - 避免慢速操作（如磁盘 IO）影响网络响应
 *
 * 设计要点：
 * - 使用无锁 MPMC 队列作为任务队列，支持多生产者多消费者
 * - 队列满时立即失败，由调用方决定重试或触发背压
 * - 优雅关闭：等待所有已提交任务执行完毕
 *
 * 与其他模块的关系：
 * - Broker 层将业务任务投递到本类
 * - 本类调用 StorageEngine 等底层模块
 */
class ThreadPool {
 public:
  /**
   * @brief 任务类型
   * 使用 std::function<void()> 支持任意可调用对象
   */
  using Task = std::function<void()>;

  /**
   * @brief 构造函数，创建工作线程池
   *
   * @param worker_count 工作线程数量，建议等于 CPU 核心数
   * @param queue_capacity 任务队列容量，必须是 2 的幂
   *
   * 注意：队列容量过小会导致 Submit 频繁失败，
   * 过大可能浪费内存。根据任务提交频率和处理速度权衡。
   */
  ThreadPool(std::size_t worker_count, std::size_t queue_capacity);

  /**
   * @brief 析构函数，优雅关闭线程池
   *
   * 等待所有已提交任务执行完毕后退出。
   * 如果有任务未完成，会阻塞直到完成。
   */
  ~ThreadPool();

  // 禁用拷贝，线程池不可复制
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  /**
   * @brief 提交任务到线程池
   *
   * 提交流程：
   * 1. 将任务放入无锁队列
   * 2. 唤醒一个空闲的工作线程
   * 3. 返回成功/失败
   *
   * @param task 要执行的任务
   * @return true 提交成功；false 队列已满
   *
   * 失败处理：
   * - 队列满时返回 false，调用方可选择重试或触发背压
   * - 也可以选择阻塞等待（当前实现不支持）
   *
   * 【建议】可以添加 SubmitWithTimeout 方法，支持超时等待
   */
  bool Submit(Task task);

  /**
   * @brief 优雅关闭线程池
   *
   * 关闭流程：
   * 1. 设置停止标志
   * 2. 唤醒所有等待的工作线程
   * 3. 等待队列中的任务执行完毕
   * 4. 工作线程退出
   */
  void Shutdown();

 private:
  /**
   * @brief 工作线程主函数
   *
   * 循环从队列中取出任务并执行，直到收到停止信号且队列为空。
   */
  void RunWorker();

  /**
   * @brief 任务队列
   * 使用无锁 MPMC 队列，支持多线程并发提交和消费
   */
  MpmcQueue<Task> queue_;

  /**
   * @brief 已提交但未执行的任务数量
   * 用于优雅关闭时判断是否所有任务已完成
   */
  std::atomic<std::size_t> queued_tasks_{0};

  /**
   * @brief 停止标志
   * 设置为 true 后，工作线程会在队列为空时退出
   */
  std::atomic<bool> stopping_{false};

  /**
   * @brief 等待互斥锁
   * 用于 Shutdown 时等待任务完成
   */
  std::mutex wait_mutex_;

  /**
   * @brief 等待条件变量
   * 工作线程执行完任务后通知，Shutdown 时等待
   */
  std::condition_variable wait_condition_;

  /**
   * @brief 工作线程列表
   * 存储所有工作线程的 std::thread 对象
   */
  std::vector<std::thread> workers_;
};

}  // namespace mq::core