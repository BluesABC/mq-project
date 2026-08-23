#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mq/core/storage_engine.h"

namespace mq::server {

class ReplicationClient {
 public:
  ReplicationClient(std::string host, std::uint16_t port, std::uint32_t timeout_ms = 1000);

  bool Fetch(const std::string& topic, std::uint32_t partition, std::uint64_t offset,
            std::uint32_t max_bytes, std::vector<core::Message>* messages);
  bool Append(const std::string& topic, std::uint32_t partition,
              const std::vector<core::Message>& messages);
  bool Heartbeat(const std::string& node_id, std::uint64_t replicated_offset);
  const std::string& lastError() const { return error_; }

 private:
  bool Call(std::uint8_t command, const std::string& topic, std::string payload,
            std::string* response_payload);

  std::string host_;
  std::uint16_t port_;
  std::uint32_t timeout_ms_;
  std::uint64_t request_id_ = 1;
  std::string error_;
};

}  // namespace mq::server
