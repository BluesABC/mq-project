#include "mq/network/tcp_connection.h"

#include <stdexcept>
#include <utility>

namespace mq::network {

TcpConnection::TcpConnection(std::uint64_t id, EventLoop* loop, core::MemoryPool* pool,
                             std::size_t read_capacity, std::size_t write_capacity)
    : id_(id), loop_(loop), read_buffer_(pool, read_capacity), write_capacity_(write_capacity) {
  if (loop_ == nullptr || !loop_->IsInLoopThread()) {
    throw std::invalid_argument("connection must be created by its event loop owner");
  }
}

TcpConnection::TcpConnection(std::uint64_t id, EventLoop* loop,
                             std::shared_ptr<core::MemoryPool> pool, std::size_t read_capacity,
                             std::size_t write_capacity)
    : id_(id),
      loop_(loop),
      pool_owner_(std::move(pool)),
      read_buffer_(pool_owner_.get(), read_capacity),
      write_capacity_(write_capacity) {
  if (loop_ == nullptr || !loop_->IsInLoopThread()) {
    throw std::invalid_argument("connection must be created by its event loop owner");
  }
}

void TcpConnection::SetReadCallback(ReadCallback callback) {
  if (!loop_->IsInLoopThread()) throw std::logic_error("read callback must be set by owner thread");
  read_callback_ = std::move(callback);
}

bool TcpConnection::DecodeRequests(std::string_view bytes, std::vector<protocol::Request>* requests,
                                   std::string* error) {
  if (!loop_->IsInLoopThread() || !IsOpen()) return false;
  return decoder_.Push(bytes, requests, error);
}

bool TcpConnection::OnReadable(std::string_view bytes) {
  if (!loop_->IsInLoopThread() || !IsOpen() || !read_buffer_.Append(bytes)) return false;
  if (!read_callback_) return true;
  // 回调返回实际消费的字节数，未消费部分保留以支持半包和业务层限速。
  const std::size_t consumed = read_callback_(shared_from_this(), read_buffer_.Readable());
  if (consumed > read_buffer_.readable_bytes()) {
    Close();
    return false;
  }
  read_buffer_.Consume(consumed);
  return true;
}

bool TcpConnection::Send(std::string bytes) {
  if (!IsOpen()) return false;
  if (loop_->IsInLoopThread()) return AppendWrite(bytes);
  // Worker 线程只投递发送任务，避免直接访问由 Reactor 独占的写队列。
  std::shared_ptr<TcpConnection> connection = shared_from_this();
  return loop_->QueueInLoop([connection, bytes = std::move(bytes)] {
    if (connection->IsOpen()) connection->AppendWrite(bytes);
  });
}

std::string_view TcpConnection::Writable() const {
  if (!loop_->IsInLoopThread()) return {};
  if (write_queue_.empty()) return {};
  const auto& front = write_queue_.front();
  return std::string_view(front).substr(front_write_offset_);
}

void TcpConnection::ConsumeWritten(std::size_t bytes) {
  if (!loop_->IsInLoopThread()) throw std::logic_error("writes must be consumed by owner thread");
  if (write_queue_.empty() || bytes > write_queue_.front().size() - front_write_offset_)
    throw std::out_of_range("written bytes exceed pending output");
  front_write_offset_ += bytes;
  queued_write_bytes_ -= bytes;
  if (front_write_offset_ == write_queue_.front().size()) {
    write_queue_.pop_front();
    front_write_offset_ = 0;
  }
}

void TcpConnection::Close() {
  if (!open_.exchange(false, std::memory_order_acq_rel)) return;
  if (!loop_->IsInLoopThread()) {
    std::shared_ptr<TcpConnection> connection = shared_from_this();
    loop_->QueueInLoop([connection] {
      connection->read_buffer_.Clear();
      connection->write_queue_.clear();
      connection->queued_write_bytes_ = 0;
      connection->front_write_offset_ = 0;
    });
    return;
  }
  read_buffer_.Clear();
  write_queue_.clear();
  queued_write_bytes_ = 0;
  front_write_offset_ = 0;
}

bool TcpConnection::AppendWrite(std::string_view bytes) {
  if (!loop_->IsInLoopThread() || !IsOpen()) return false;
  if (bytes.size() > write_capacity_ - queued_write_bytes_) return false;
  write_queue_.emplace_back(bytes);
  queued_write_bytes_ += bytes.size();
  return true;
}

}  // namespace mq::network
