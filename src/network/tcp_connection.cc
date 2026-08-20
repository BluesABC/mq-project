#include "mq/network/tcp_connection.h"

#include <stdexcept>
#include <utility>

namespace mq::network {

TcpConnection::TcpConnection(std::uint64_t id, EventLoop* loop, core::MemoryPool* pool,
                             std::size_t read_capacity, std::size_t write_capacity)
    : id_(id), loop_(loop), read_buffer_(pool, read_capacity), write_buffer_(pool, write_capacity) {
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
  std::shared_ptr<TcpConnection> connection = shared_from_this();
  return loop_->QueueInLoop([connection, bytes = std::move(bytes)] {
    if (connection->IsOpen()) connection->AppendWrite(bytes);
  });
}

std::string_view TcpConnection::Writable() const {
  if (!loop_->IsInLoopThread()) return {};
  return write_buffer_.Readable();
}

void TcpConnection::ConsumeWritten(std::size_t bytes) {
  if (!loop_->IsInLoopThread()) throw std::logic_error("writes must be consumed by owner thread");
  write_buffer_.Consume(bytes);
}

void TcpConnection::Close() {
  if (!open_.exchange(false, std::memory_order_acq_rel)) return;
  if (!loop_->IsInLoopThread()) {
    std::shared_ptr<TcpConnection> connection = shared_from_this();
    loop_->QueueInLoop([connection] { connection->read_buffer_.Clear(); connection->write_buffer_.Clear(); });
    return;
  }
  read_buffer_.Clear();
  write_buffer_.Clear();
}

bool TcpConnection::AppendWrite(std::string_view bytes) {
  if (!loop_->IsInLoopThread() || !IsOpen()) return false;
  return write_buffer_.Append(bytes);
}

}  // namespace mq::network
