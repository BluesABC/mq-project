#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "mq/network/buffer.h"
#include "mq/network/event_loop.h"
#include "mq/protocol/protocol_codec.h"

namespace mq::network {

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  using ReadCallback = std::function<std::size_t(std::shared_ptr<TcpConnection>, std::string_view)>;

  TcpConnection(std::uint64_t id, EventLoop* loop, core::MemoryPool* pool,
                std::size_t read_capacity, std::size_t write_capacity);

  std::uint64_t id() const { return id_; }
  bool IsOpen() const { return open_.load(std::memory_order_acquire); }

  void SetReadCallback(ReadCallback callback);
  bool DecodeRequests(std::string_view bytes, std::vector<protocol::Request>* requests,
                      std::string* error = nullptr);
  bool OnReadable(std::string_view bytes);
  bool Send(std::string bytes);
  std::string_view Writable() const;
  void ConsumeWritten(std::size_t bytes);
  void Close();

 private:
  bool AppendWrite(std::string_view bytes);

  const std::uint64_t id_;
  EventLoop* const loop_;
  Buffer read_buffer_;
  Buffer write_buffer_;
  ReadCallback read_callback_;
  protocol::RequestStreamDecoder decoder_;
  std::atomic<bool> open_{true};
};

}  // namespace mq::network
