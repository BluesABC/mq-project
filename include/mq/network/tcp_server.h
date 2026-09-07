#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "mq/network/event_loop.h"
#include "mq/network/tls.h"
#include "mq/protocol/commands.h"

namespace mq::network {

/**
 * @brief TCP 服务器
 *
 * 负责监听端口、接受连接、分发请求。
 *
 * 在整体架构中的角色：
 * - 监听客户端连接请求
 * - 为每个连接分配 EventLoop 和 TcpConnection
 * - 将协议解析后的请求交给 RequestHandler 处理
 *
 * 设计要点：
 * - 主线程负责 Accept
 * - 子线程（EventLoop）负责处理连接的读写事件
 * - 使用 Reactor 模式，支持高并发连接
 *
 * 与其他模块的关系：
 * - Broker 实现 RequestHandler，处理业务请求
 * - TcpConnection 负责单条连接的状态管理
 * - EventLoop 驱动所有网络 IO
 *
 * 线程模型：
 * ```
 * Main Thread (Accept)
 *     │
 *     ├── EventLoop 1 (Sub Reactor)
 *     ├── EventLoop 2 (Sub Reactor)
 *     └── EventLoop N (Sub Reactor)
 * ```
 *
 * 注意事项：
 * - 端口绑定后不可更改
 * - Stop 会等待所有连接关闭
 * - TLS 可选，通过 TlsOptions 配置
 */
class TcpServer {
 public:
  /**
   * @brief 请求处理回调类型
   *
   * 接收协议请求，返回协议响应。
   * Broker 的 Handle 方法实现了此接口。
   */
  using RequestHandler = std::function<protocol::Response(const protocol::Request&)>;

  /**
   * @brief 构造函数，绑定所有网络接口
   *
   * @param port 监听端口
   * @param worker_count EventLoop 线程数
   * @param handler 请求处理回调
   * @param tls_options TLS 配置（可选）
   */
  TcpServer(std::uint16_t port, std::size_t worker_count, RequestHandler handler,
            TlsOptions tls_options = {});

  /**
   * @brief 构造函数，绑定指定网络接口
   *
   * @param bind_address 绑定的 IP 地址，如 "127.0.0.1"
   * @param port 监听端口
   * @param worker_count EventLoop 线程数
   * @param handler 请求处理回调
   * @param tls_options TLS 配置（可选）
   */
  TcpServer(std::string bind_address, std::uint16_t port, std::size_t worker_count,
            RequestHandler handler, TlsOptions tls_options = {});

  /**
   * @brief 析构函数，优雅关闭服务器
   *
   * 关闭流程：
   * 1. 停止接受新连接
   * 2. 等待所有现有连接关闭
   * 3. 释放所有资源
   */
  ~TcpServer();

  /**
   * @brief 启动服务器
   *
   * 启动流程：
   * 1. 创建监听 socket
   * 2. 绑定地址和端口
   * 3. 开始监听
   * 4. 启动所有 EventLoop 线程
   *
   * @return true 启动成功；false 端口被占用或系统错误
   */
  bool Start();

  /**
   * @brief 停止服务器
   *
   * 停止流程：
   * 1. 关闭监听 socket，停止接受新连接
   * 2. 通知所有 EventLoop 停止
   * 3. 等待所有 EventLoop 线程退出
   */
  void Stop();

  /**
   * @brief 获取监听的端口号
   * @return 端口号
   */
  std::uint16_t port() const;

 private:
  /**
   * @brief 内部实现结构体
   * 使用 Pimpl 模式隐藏实现细节，减少编译依赖
   */
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace mq::network