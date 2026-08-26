#pragma once

#include <cstddef>
#include <string_view>

#include "mq/core/memory_pool.h"

namespace mq::network {

// 网络缓冲区从 MemoryPool 获取固定容量，避免收包过程中反复分配。
class Buffer {
 public:
  Buffer(core::MemoryPool* pool, std::size_t capacity_bytes);

  bool Append(std::string_view data);
  std::string_view Readable() const;
  // 消费后只移动读指针；空间不足时由 Append 触发紧凑整理。
  void Consume(std::size_t bytes);
  void Clear();
  std::size_t readable_bytes() const {
    return write_position_ - read_position_;
  }
  std::size_t writable_bytes() const {
    return capacity_bytes_ - write_position_;
  }

 private:
  void Compact();

  char* data_ = nullptr;
  std::size_t capacity_bytes_ = 0;
  std::size_t read_position_ = 0;
  std::size_t write_position_ = 0;
};

}  // namespace mq::network
