#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "mq/network/buffer.h"
#include "mq/network/event_loop.h"
#include "mq/protocol/protocol_codec.h"

namespace mq::network {

/**
 * @brief TCP 连接
 *
 * 封装单条 TCP 连接的收发缓冲和协议解码。
 *
 * 设计要点：
 * 1. 单线程访问：所有状态都应由所属 EventLoop 线程访问
 * 2. 异步发送：Send 只追加到内存队列，真正发送由 Reactor 驱动
 * 3. 协议解码：内置 RequestStreamDecoder 处理 TCP 粘包/半包
 *
 * 在整体架构中的角色：
 * - 管理单条连接的读写缓冲
 * - 协议解码，将字节流转换为 Request 对象
 * - 发送队列管理，控制流量
 *
 * 与其他模块的关系：
 * - EventLoop 负责驱动本类的读写事件
 * - ProtocolCodec 负责协议帧的编解码
 * - Broker 通过 RequestHandler 处理解码后的请求
 *
 * 生命周期：
 * 1. Accept 新连接时创建
 * 2. 绑定到一个 EventLoop
 * 3. 处理读写事件直到连接关闭
 * 4. 关闭后释放资源
 *
 * 注意事项：
 * - 禁止跨线程调用 Send，必须通过 EventLoop::QueueInLoop
 * - 发送队列满时 Send 会失败，需要处理背压
 * - 连接关闭后应立即释放，避免悬空指针
 */
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  /**
   * @brief 数据可读回调类型
   *
   * @param conn 连接对象
   * @param data 接收到的数据
   * @return 消费的字节数
   */
  using ReadCallback = std::function<std::size_t(std::shared_ptr<TcpConnection>, std::string_view)>;

  /**
   * @brief 构造函数（使用原始指针管理 MemoryPool）
   *
   * @param id 连接 ID（唯一标识）
   * @param loop 所属的 EventLoop
   * @param pool 内存池，用于分配读写缓冲
   * @param read_capacity 读缓冲区容量（字节）
   * @param write_capacity 写缓冲区容量（字节）
   */
  TcpConnection(std::uint64_t id, EventLoop* loop, core::MemoryPool* pool,
                std::size_t read_capacity, std::size_t write_capacity);

  /**
   * @brief 构造函数（使用 shared_ptr 管理 MemoryPool）
   *
   * @param id 连接 ID
   * @param loop 所属的 EventLoop
   * @param pool 内存池（shared_ptr 版本）
   * @param read_capacity 读缓冲区容量（字节）
   * @param write_capacity 写缓冲区容量（字节）
   */
  TcpConnection(std::uint64_t id, EventLoop* loop, std::shared_ptr<core::MemoryPool> pool,
                std::size_t read_capacity, std::size_t write_capacity);

  /**
   * @brief 获取连接 ID
   * @return 唯一标识符
   */
  std::uint64_t id() const {
    return id_;
  }

  /**
   * @brief 检查连接是否打开
   * @return true 连接打开；false 已关闭
   */
  bool IsOpen() const {
    return open_.load(std::memory_order_acquire);
  }

  /**
   * @brief 设置数据可读回调
   *
   * 当有新数据到达时调用此回调。
   *
   * @param callback 回调函数
   */
  void SetReadCallback(ReadCallback callback);

  /**
   * @brief 解码协议请求
   *
   * 处理 TCP 粘包/半包问题，将字节流转换为 Request 对象。
   *
   * @param bytes 接收到的原始字节
   * @param requests 输出参数，解码后的请求列表
   * @param error 可选的错误信息输出参数
   * @return true 解码成功；false 协议错误
   */
  bool DecodeRequests(std::string_view bytes, std::vector<protocol::Request>* requests,
                      std::string* error = nullptr);

  /**
   * @brief 处理可读事件
   *
   * 流程：
   * 1. 将数据追加到读缓冲区
   * 2. 解码协议请求
   * 3. 调用读回调处理请求
   *
   * @param bytes 接收到的数据
   * @return true 处理成功；false 连接已关闭
   */
  bool OnReadable(std::string_view bytes);

  /**
   * @brief 发送数据
   *
   * 注意：此方法只追加到内存队列，不执行阻塞写。
   * 真正的发送由 Reactor 的可写事件驱动。
   *
   * 线程安全：应从 EventLoop 线程调用，其他线程需通过 QueueInLoop
   *
   * @param bytes 要发送的数据
   * @return true 追加成功；false 发送队列已满或连接已关闭
   */
  bool Send(std::string bytes);

  /**
   * @brief 获取可发送的数据
   *
   * 返回发送队列中待发送的数据，用于 Reactor 执行实际发送。
   *
   * @return 待发送数据的字符串视图
   */
  std::string_view Writable() const;

  /**
   * @brief 标记已发送的数据
   *
   * 发送完成后调用，从队列中移除已发送的部分。
   *
   * @param bytes 已发送的字节数
   */
  void ConsumeWritten(std::size_t bytes);

  /**
   * @brief 关闭连接
   *
   * 关闭流程：
   * 1. 设置关闭标志
   * 2. 关闭 socket
   * 3. 清空缓冲区
   * 4. 通知 EventLoop 移除 fd
   */
  void Close();

 private:
  /**
   * @brief 追加数据到发送队列
   *
   * @param bytes 要追加的数据
   * @return true 追加成功；false 队列已满
   */
  bool AppendWrite(std::string_view bytes);

  /**
   * @brief 连接 ID
   */
  const std::uint64_t id_;

  /**
   * @brief 所属的 EventLoop
   * 使用原始指针，因为 EventLoop 的生命周期由 TcpServer 管理
   */
  EventLoop* const loop_;

  /**
   * @brief 内存池所有者
   * 使用 shared_ptr 确保生命周期与连接一致
   */
  std::shared_ptr<core::MemoryPool> pool_owner_;

  /**
   * @brief 读缓冲区
   * 从内存池分配，避免频繁分配/释放
   */
  Buffer read_buffer_;

  /**
   * @brief 发送队列容量上限（字节）
   * 用于背压控制，防止发送队列无限增长
   */
  const std::size_t write_capacity_;

  /**
   * @brief 发送队列
   * 使用 deque 支持高效的头部移除
   */
  std::deque<std::string> write_queue_;

  /**
   * @brief 发送队列中待发送的总字节数
   * 用于判断是否超过容量上限
   */
  std::size_t queued_write_bytes_ = 0;

  /**
   * @brief 队首元素的读偏移量
   * 用于部分发送场景
   */
  std::size_t front_write_offset_ = 0;

  /**
   * @brief 数据可读回调
   */
  ReadCallback read_callback_;

  /**
   * @brief 协议流解码器
   * 处理 TCP 粘包/半包问题
   */
  protocol::RequestStreamDecoder decoder_;

  /**
   * @brief 连接打开标志
   * 使用原子变量支持安全的多线程检查
   */
  std::atomic<bool> open_{true};
};

}  // namespace mq::network