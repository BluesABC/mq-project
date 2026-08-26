#include "mq/network/buffer.h"

#include <cstring>
#include <stdexcept>

namespace mq::network {

Buffer::Buffer(core::MemoryPool* pool, std::size_t capacity_bytes)
    : capacity_bytes_(capacity_bytes) {
  if (pool == nullptr || capacity_bytes == 0 || !pool->IsOwnerThread()) {
    throw std::invalid_argument("buffer must be created by its memory pool owner");
  }
  data_ = static_cast<char*>(pool->Allocate(capacity_bytes, alignof(char)));
  if (data_ == nullptr) throw std::bad_alloc();
}

bool Buffer::Append(std::string_view data) {
  if (data.size() > capacity_bytes_ - readable_bytes()) return false;
  if (data.size() > writable_bytes()) Compact();
  std::memcpy(data_ + write_position_, data.data(), data.size());
  write_position_ += data.size();
  return true;
}

std::string_view Buffer::Readable() const {
  return std::string_view(data_ + read_position_, readable_bytes());
}

void Buffer::Consume(std::size_t bytes) {
  if (bytes >= readable_bytes()) {
    Clear();
    return;
  }
  read_position_ += bytes;
}

void Buffer::Clear() {
  read_position_ = 0;
  write_position_ = 0;
}

void Buffer::Compact() {
  const std::size_t bytes = readable_bytes();
  std::memmove(data_, data_ + read_position_, bytes);
  read_position_ = 0;
  write_position_ = bytes;
}

}  // namespace mq::network
