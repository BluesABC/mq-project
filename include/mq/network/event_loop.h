#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "mq/core/buffer.h"

namespace mq::network {

/**
 * @brief 事件循环（Reactor 核心）
 *
 * 将文件描述符事件和跨线程任务串行化到所属 Reactor 线程。
 *
 * 设计要点：
 * 1. 单线程执行：所有任务和事件回调都在同一个线程中执行
 * 2. 无锁设计：连接对象只能在该线程操作，避免锁竞争
 * 3. 跨线程投递：其他线程通过 QueueInLoop 投递任务，不直接碰连接状态
 *
 * 在整体架构中的角色：
 * - 主 Reactor 负责 Accept 新连接
 * - 子 Reactor 负责处理已建立连接的读写事件
 * - 本类是子 Reactor 的核心，驱动所有网络 IO
 *
 * 与其他模块的关系：
 * - TcpServer 创建并管理多个 EventLoop
 * - TcpConnection 绑定到一个 EventLoop
 * - ThreadPool 可以接收来自 EventLoop 的任务
 *
 * 线程模型：
 * ```
 * Main Reactor (Accept)
 *     │
 *     ├── Sub Reactor 1 (EventLoop 1)
 *     ├── Sub Reactor 2 (EventLoop 2)
 *     └── Sub Reactor N (EventLoop N)
 * ```
 *
 * 注意事项：
 * - 禁止在事件回调中执行阻塞操作
 * - 网络层禁止文件 IO、锁竞争、日志落盘
 * - 所有慢速操作必须投递到 ThreadPool
 */
class EventLoop {
 public:
  /**
   * @brief 任务类型
   * 使用 std::function<void()> 支持任意可调用对象
   */
  using Task = std::function<void()>;

  /**
   * @brief 文件描述符事件回调
   * 参数为事件类型（EPOLLIN/EPOLLOUT 等）
   */
  using FdCallback = std::function<void(std::uint32_t)>;

  /**
   * @brief 构造函数
   *
   * @param queue_capacity 跨线程任务队列容量，必须是 2 的幂
   */
  explicit EventLoop(std::size_t queue_capacity);

  /**
   * @brief 析构函数，停止事件循环并释放资源
   */
  ~EventLoop();

  // 禁用拷贝，EventLoop 不可复制
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  /**
   * @brief 启动事件循环
   *
   * 启动流程：
   * 1. 创建 epoll 实例（Linux）或使用 select（Windows）
   * 2. 创建唤醒 fd
   * 3. 启动事件循环线程
   *
   * @return true 启动成功；false 已启动或系统资源不足
   */
  bool Start();

  /**
   * @brief 设置空闲回调
   *
   * 当没有事件和任务时调用，可用于执行低优先级任务。
   *
   * @param callback 空闲时调用的回调函数
   * @return true 设置成功；false 已启动
   */
  bool SetIdleCallback(Task callback);

  /**
   * @brief 将任务投递到事件循环线程执行
   *
   * 投递流程：
   * 1. 将任务放入无锁队列
   * 2. 通过 eventfd 或条件变量唤醒事件循环
   * 3. 事件循环在下一次迭代时执行任务
   *
   * 线程安全：可从任意线程调用
   *
   * @param task 要执行的任务
   * @return true 投递成功；false 队列已满
   */
  bool QueueInLoop(Task task);

  /**
   * @brief 注册文件描述符到 epoll
   *
   * 注册后，当 fd 上发生指定事件时会调用回调。
   *
   * @param fd 文件描述符
   * @param events 监听的事件类型（EPOLLIN | EPOLLOUT 等）
   * @param callback 事件触发时的回调函数
   * @return true 注册成功；false fd 无效或已注册
   */
  bool RegisterFd(int fd, std::uint32_t events, FdCallback callback);

  /**
   * @brief 修改 fd 监听的事件类型
   *
   * 通常用于切换读/写监听。
   *
   * @param fd 文件描述符
   * @param events 新的事件类型
   * @return true 修改成功；false fd 未注册
   */
  bool ModifyFd(int fd, std::uint32_t events);

  /**
   * @brief 从 epoll 移除 fd
   *
   * @param fd 文件描述符
   * @return true 移除成功；false fd 未注册
   */
  bool RemoveFd(int fd);

  /**
   * @brief 检查当前线程是否是事件循环线程
   *
   * 用于调试和断言，确保在正确的线程操作。
   *
   * @return true 当前线程是事件循环线程；false 不是
   */
  bool IsInLoopThread() const;

  /**
   * @brief 停止事件循环
   *
   * 停止流程：
   * 1. 设置停止标志
   * 2. 唤醒事件循环
   * 3. 等待事件循环线程退出
   */
  void Stop();

 private:
  /**
   * @brief 事件循环主函数
   *
   * 循环流程：
   * 1. 等待 epoll 事件或跨线程任务
   * 2. 处理所有就绪的 fd 事件
   * 3. 执行所有跨线程任务
   * 4. 如果没有事件和任务，调用空闲回调
   */
  void Run();

  /**
   * @brief 跨线程任务队列
   * 使用无锁 MPMC 队列，支持多线程并发投递
   */
  core::MpmcQueue<Task> queue_;

  /**
   * @brief 已投递但未执行的任务数量
   */
  std::atomic<std::size_t> queued_tasks_{0};

  /**
   * @brief 启动标志
   */
  std::atomic<bool> started_{false};

  /**
   * @brief 停止标志
   */
  std::atomic<bool> stopping_{false};

  /**
   * @brief 等待互斥锁
   */
  mutable std::mutex wait_mutex_;

  /**
   * @brief 等待条件变量
   */
  std::condition_variable wait_condition_;

  /**
   * @brief 空闲回调
   */
  Task idle_callback_;

  /**
   * @brief 所有者线程互斥锁
   */
  mutable std::mutex owner_mutex_;

  /**
   * @brief 事件循环所属线程 ID
   */
  std::thread::id owner_thread_id_;

  /**
   * @brief 事件循环线程
   */
  std::thread thread_;

#ifndef _WIN32
  /**
   * @brief epoll 文件描述符（仅 Linux）
   */
  int epoll_fd_ = -1;

  /**
   * @brief 唤醒文件描述符（仅 Linux）
   * 用于从其他线程唤醒事件循环
   */
  int wake_fd_ = -1;

  /**
   * @brief fd 回调映射互斥锁
   */
  std::mutex fd_mutex_;

  /**
   * @brief fd 到回调的映射表
   */
  std::unordered_map<int, FdCallback> fd_callbacks_;
#endif
};

}  // namespace mq::network