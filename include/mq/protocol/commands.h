#pragma once

#include <cstdint>
#include <string>

namespace mq::protocol {

// 协议常量集中定义，避免客户端和 Broker 对帧格式、版本或长度上限产生分歧。
constexpr std::uint16_t kMagic = 0x4D51;
constexpr std::uint8_t kCurrentVersion = 1;
constexpr std::uint32_t kMaxPayloadBytes = 1024 * 1024;

enum class Command : std::uint8_t {
  kCreateTopic = 0x01,
  kDeleteTopic = 0x02,
  kListTopic = 0x03,
  kMetrics = 0x04,
  kProduce = 0x10,
  kProduceBatch = 0x11,
  kFetch = 0x20,
  kCommitOffset = 0x21,
  kHeartbeat = 0x30,
  kReplicaFetch = 0x40,
  kReplicaAppend = 0x41,
  kReplicaVote = 0x42,
};

// 状态码由协议层统一承载，业务层不应直接返回魔法数字。
enum class Status : std::uint8_t {
  kOk = 0x00,
  kBadRequest = 0x10,
  kUnknownTopic = 0x11,
  kTopicExists = 0x12,
  kInvalidOffset = 0x13,
  kStorageError = 0x14,
  kVersionMismatch = 0x15,
  kNotSupported = 0x16,
  kNotLeader = 0x17,
  kRateLimited = 0x18,
  kQuotaExceeded = 0x19,
  kResourceExhausted = 0x1A,
  kUnauthenticated = 0x1B,
  kPermissionDenied = 0x1C,
  kInternalError = 0x20,
};

constexpr std::uint16_t kAckMask = 0x0003;
constexpr std::uint16_t kAckZero = 0x0000;
constexpr std::uint16_t kAckOne = 0x0001;
constexpr std::uint16_t kAckAll = 0x0002;
constexpr std::uint16_t kFlagProducerMetadata = 0x0004;
constexpr std::uint16_t kFlagReplication = 0x0008;
constexpr std::uint16_t kFlagReplicationTerm = 0x0010;
// 开启客户端鉴权时，载荷前缀为 token_len(2) | token；业务载荷布局保持不变。
constexpr std::uint16_t kFlagAuthentication = 0x0020;

struct Request {
  // 请求和响应都携带 request_id，客户端可据此匹配异步响应并实现幂等。
  std::uint8_t version = kCurrentVersion;
  Command command = Command::kHeartbeat;
  std::uint64_t request_id = 0;
  std::uint16_t flags = 0;
  std::string topic;
  std::string payload;
};

struct Response {
  std::uint8_t version = kCurrentVersion;
  Status status = Status::kOk;
  std::uint64_t request_id = 0;
  std::uint16_t flags = 0;
  std::string payload;
};

}  // namespace mq::protocol
