#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mq/core/storage_engine.h"

namespace mq::server {

// 使用协议命令与远端 Broker 通信；复制失败通过返回值和 lastError 交给协调层处理。
class ReplicationClient {
 public:
  ReplicationClient(std::string host, std::uint16_t port, std::uint32_t timeout_ms = 1000);

  bool Fetch(const std::string& topic, std::uint32_t partition, std::uint64_t offset,
             std::uint32_t max_bytes, std::vector<core::Message>* messages);
  bool Append(const std::string& topic, std::uint32_t partition,
              const std::vector<core::Message>& messages, std::uint64_t term = 0,
              std::uint64_t commit_index = 0, const std::string& leader_id = {});
  bool Heartbeat(const std::string& node_id, std::uint64_t replicated_offset,
                 std::uint64_t term = 0, std::uint64_t commit_index = 0);
  bool Vote(std::uint64_t term, const std::string& candidate_id, bool* granted);
  const std::string& lastError() const {
    return error_;
  }

 private:
  bool Call(std::uint8_t command, const std::string& topic, std::string payload,
            std::string* response_payload, bool term_payload = false);

  std::string host_;
  std::uint16_t port_;
  std::uint32_t timeout_ms_;
  std::uint64_t request_id_ = 1;
  std::string error_;
};

}  // namespace mq::server
