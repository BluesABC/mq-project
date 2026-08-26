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

// TcpConnection 封装单条 TCP 连接的收发缓冲和协议解码。
// 其所有状态都应由所属 EventLoop 线程访问，发送队列满时通过 Send 返回失败。
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  using ReadCallback = std::function<std::size_t(std::shared_ptr<TcpConnection>, std::string_view)>;

  TcpConnection(std::uint64_t id, EventLoop* loop, core::MemoryPool* pool,
                std::size_t read_capacity, std::size_t write_capacity);
  TcpConnection(std::uint64_t id, EventLoop* loop, std::shared_ptr<core::MemoryPool> pool,
                std::size_t read_capacity, std::size_t write_capacity);

  std::uint64_t id() const {
    return id_;
  }
  bool IsOpen() const {
    return open_.load(std::memory_order_acquire);
  }

  void SetReadCallback(ReadCallback callback);
  bool DecodeRequests(std::string_view bytes, std::vector<protocol::Request>* requests,
                      std::string* error = nullptr);
  bool OnReadable(std::string_view bytes);
  // Send 只追加到内存队列，不执行阻塞写；真正发送由 Reactor 的可写事件驱动。
  bool Send(std::string bytes);
  std::string_view Writable() const;
  void ConsumeWritten(std::size_t bytes);
  void Close();

 private:
  bool AppendWrite(std::string_view bytes);

  const std::uint64_t id_;
  EventLoop* const loop_;
  std::shared_ptr<core::MemoryPool> pool_owner_;
  Buffer read_buffer_;
  const std::size_t write_capacity_;
  std::deque<std::string> write_queue_;
  std::size_t queued_write_bytes_ = 0;
  std::size_t front_write_offset_ = 0;
  ReadCallback read_callback_;
  protocol::RequestStreamDecoder decoder_;
  std::atomic<bool> open_{true};
};

}  // namespace mq::network
