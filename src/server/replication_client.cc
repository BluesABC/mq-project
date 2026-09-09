// MQ 复制客户端实现
//
// ReplicationClient 负责与远程 Broker 节点进行复制通信：
// 1. 数据拉取：从 Leader 拉取增量消息（REPLICA_FETCH）
// 2. 数据追加：向 Follower 追加消息（REPLICA_APPEND）
// 3. 心跳通信：报告复制进度和接收 Leader 状态
// 4. 选举投票：参与预投票和正式投票
//
// 设计特点：
// - 使用短连接，故障时可以独立重试
// - 所有请求携带认证 Token，确保安全性
// - 支持 Windows 和 Linux 跨平台
// - 请求/响应使用大端序二进制协议

#include "mq/server/replication_client.h"

#include <chrono>
#include <cstring>
#include <string_view>

#include "mq/protocol/protocol_codec.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using Socket = SOCKET;
constexpr Socket kInvalidSocket = INVALID_SOCKET;
#else
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
using Socket = int;
constexpr Socket kInvalidSocket = -1;
#endif

namespace mq::server {
namespace {

// 关闭 Socket
void CloseSocket(Socket socket) {
#ifdef _WIN32
  closesocket(socket);
#else
  close(socket);
#endif
}

// 向字符串追加 16 位大端序整数
void Put16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}

// 向字符串追加 32 位大端序整数
void Put32(std::string* out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}

// 向字符串追加 64 位大端序整数
void Put64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}

// 从字符串视图中读取 16 位大端序整数
std::uint16_t Get16(std::string_view data, std::size_t pos) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(data[pos])) << 8) |
         static_cast<unsigned char>(data[pos + 1]);
}

// 从字符串视图中读取 32 位大端序整数
std::uint32_t Get32(std::string_view data, std::size_t pos) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}

// 从字符串视图中读取 64 位大端序整数
std::uint64_t Get64(std::string_view data, std::size_t pos) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}

// 发送所有数据，处理部分发送情况
bool SendAll(Socket socket, std::string_view data) {
  std::size_t sent = 0;
  while (sent < data.size()) {
    const int count = send(socket, data.data() + sent, static_cast<int>(data.size() - sent), 0);
    if (count <= 0) return false;
    sent += static_cast<std::size_t>(count);
  }
  return true;
}

// 接收所有数据，处理部分接收情况
bool ReceiveAll(Socket socket, char* data, std::size_t size) {
  std::size_t received = 0;
  while (received < size) {
    const int count = recv(socket, data + received, static_cast<int>(size - received), 0);
    if (count <= 0) return false;
    received += static_cast<std::size_t>(count);
  }
  return true;
}

// 设置 Socket 超时时间
void SetTimeout(Socket socket, std::uint32_t timeout_ms) {
#ifdef _WIN32
  const int value = static_cast<int>(timeout_ms);
  setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&value), sizeof(value));
  setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&value), sizeof(value));
#else
  timeval value{};
  value.tv_sec = timeout_ms / 1000;
  value.tv_usec = (timeout_ms % 1000) * 1000;
  setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value));
  setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value));
#endif
}

}  // namespace

// 构造函数，初始化复制客户端
ReplicationClient::ReplicationClient(std::string host, std::uint16_t port, std::uint32_t timeout_ms,
                                     std::string auth_token)
    : host_(std::move(host)),
      port_(port),
      timeout_ms_(timeout_ms == 0 ? 1000 : timeout_ms),
      auth_token_(std::move(auth_token)) {}

// 核心 RPC 调用方法
// 负责建立连接、发送请求、接收响应和解析响应
bool ReplicationClient::Call(std::uint8_t command, const std::string& topic, std::string payload,
                             std::string* response_payload, bool term_payload) {
  // 检查认证 Token
  if (auth_token_.empty() || auth_token_.size() > UINT16_MAX) {
    error_ = "replication authentication token is not configured";
    return false;
  }
  // 构造认证载荷：Token 长度 + Token + 实际载荷
  std::string authenticated_payload;
  Put16(&authenticated_payload, static_cast<std::uint16_t>(auth_token_.size()));
  authenticated_payload.append(auth_token_);
  authenticated_payload.append(std::move(payload));
  // Windows 初始化
#ifdef _WIN32
  static bool initialized = false;
  if (!initialized) {
    WSADATA data{};
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
      error_ = "WSAStartup failed";
      return false;
    }
    initialized = true;
  }
#endif
  // 创建 Socket
  Socket socket = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socket == kInvalidSocket) {
    error_ = "socket failed";
    return false;
  }
  // DNS 解析
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  if (getaddrinfo(host_.c_str(), std::to_string(port_).c_str(), &hints, &result) != 0 ||
      result == nullptr) {
    CloseSocket(socket);
    error_ = "resolve failed";
    return false;
  }
  // 设置超时并连接
  SetTimeout(socket, timeout_ms_);
  const bool connected =
      ::connect(socket, result->ai_addr, static_cast<int>(result->ai_addrlen)) == 0;
  freeaddrinfo(result);
  if (!connected) {
    CloseSocket(socket);
    error_ = "connect failed";
    return false;
  }
  // 构造请求
  protocol::Request request;
  request.command = static_cast<protocol::Command>(command);
  request.request_id = request_id_++;
  request.flags = protocol::kFlagReplication | (term_payload ? protocol::kFlagReplicationTerm : 0);
  request.topic = topic;
  request.payload = std::move(authenticated_payload);
  // 编码并发送请求
  std::string frame;
  protocol::Response response;
  const auto request_id = request.request_id;
  bool okay = protocol::ProtocolCodec::EncodeRequest(request, &frame) && SendAll(socket, frame);
  // 接收响应头
  char header[18]{};
  if (okay && ReceiveAll(socket, header, sizeof(header))) {
    const auto payload_size = Get32(std::string_view(header, sizeof(header)), 14);
    if (payload_size <= protocol::kMaxPayloadBytes) {
      // 接收完整响应
      std::string full(header, sizeof(header));
      full.resize(18 + payload_size);
      okay = ReceiveAll(socket, full.data() + 18, payload_size) &&
             protocol::ProtocolCodec::DecodeResponse(full, &response, &error_) &&
             response.request_id == request_id;
    } else
      okay = false;
  } else
    okay = false;
  CloseSocket(socket);
  // 检查响应状态
  if (!okay) {
    if (error_.empty()) error_ = "replication response failed";
    return false;
  }
  if (response.status != protocol::Status::kOk) {
    error_ = "replication request rejected";
    return false;
  }
  if (response_payload != nullptr) *response_payload = std::move(response.payload);
  error_.clear();
  return true;
}

// 从 Leader 拉取增量消息
// Follower 使用此方法获取新的消息
bool ReplicationClient::Fetch(const std::string& topic, std::uint32_t partition,
                              std::uint64_t offset, std::uint32_t max_bytes,
                              std::vector<core::Message>* messages) {
  if (messages == nullptr || max_bytes == 0) return false;
  // 构造请求载荷：分区 + 偏移量 + 最大字节数
  std::string payload;
  Put32(&payload, partition);
  Put64(&payload, offset);
  Put32(&payload, max_bytes);
  std::string response;
  if (!Call(static_cast<std::uint8_t>(protocol::Command::kReplicaFetch), topic, std::move(payload),
            &response) ||
      response.size() < 4)
    return false;
  // 解析响应：消息数量 + 消息列表
  std::size_t position = 4;
  const auto count = Get32(response, 0);
  messages->clear();
  if (count > protocol::kMaxPayloadBytes / 18) {
    error_ = "invalid replica fetch response count";
    return false;
  }
  messages->reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    if (position + 18 > response.size()) {
      error_ = "invalid replica fetch response";
      return false;
    }
    core::Message message;
    message.offset = Get64(response, position);
    position += 8;
    message.timestamp_ms = static_cast<std::int64_t>(Get64(response, position));
    position += 8;
    const auto key_size = Get16(response, position);
    position += 2;
    if (position + key_size + 4 > response.size()) return false;
    message.key.assign(response, position, key_size);
    position += key_size;
    const auto value_size = Get32(response, position);
    position += 4;
    if (position + value_size > response.size()) return false;
    message.value.assign(response, position, value_size);
    position += value_size;
    messages->push_back(std::move(message));
  }
  return position == response.size();
}

// 向 Follower 追加消息
// Leader 使用此方法同步数据给 Follower
bool ReplicationClient::Append(const std::string& topic, std::uint32_t partition,
                               const std::vector<core::Message>& messages, std::uint64_t term,
                               std::uint64_t commit_index, const std::string& leader_id,
                               std::uint64_t prev_log_index, std::uint64_t prev_log_term,
                               std::uint64_t* next_log_index) {
  if (messages.empty() || leader_id.empty() || leader_id.size() > UINT16_MAX) return false;
  // 构造请求载荷
  std::string payload;
  Put64(&payload, term);
  Put64(&payload, commit_index);
  Put16(&payload, static_cast<std::uint16_t>(leader_id.size()));
  payload.append(leader_id);
  Put64(&payload, prev_log_index);
  Put64(&payload, prev_log_term);
  Put32(&payload, partition);
  Put32(&payload, static_cast<std::uint32_t>(messages.size()));
  // 编码消息列表
  for (const auto& message : messages) {
    Put64(&payload, message.offset);
    Put64(&payload, static_cast<std::uint64_t>(message.timestamp_ms));
    Put16(&payload, static_cast<std::uint16_t>(message.key.size()));
    payload.append(message.key);
    Put32(&payload, static_cast<std::uint32_t>(message.value.size()));
    payload.append(message.value);
  }
  std::string response;
  const bool success = Call(static_cast<std::uint8_t>(protocol::Command::kReplicaAppend), topic,
                             std::move(payload), &response, true);
  // 如果失败且返回了 next_log_index，用于日志冲突恢复
  if (!success && next_log_index != nullptr && response.size() == 8) *next_log_index = Get64(response, 0);
  return success;
}

// 发送心跳
// 报告复制进度或接收 Leader 状态
bool ReplicationClient::Heartbeat(const std::string& topic, std::uint32_t partition,
                                  const std::string& node_id, std::uint64_t replicated_offset,
                                  std::uint64_t term, std::uint64_t commit_index) {
  if (node_id.empty() || node_id.size() > UINT16_MAX) {
    error_ = "invalid replication node id";
    return false;
  }
  // 构造心跳载荷
  std::string payload;
  Put16(&payload, static_cast<std::uint16_t>(node_id.size()));
  payload.append(node_id);
  Put32(&payload, partition);
  Put64(&payload, replicated_offset);
  // 如果携带 term，则是 Leader 发送的心跳
  if (term != 0) {
    Put64(&payload, term);
    Put64(&payload, commit_index);
  }
  return Call(static_cast<std::uint8_t>(protocol::Command::kHeartbeat), topic, std::move(payload),
              nullptr, term != 0);
}

// 请求投票
// 候选节点请求其他节点投票
bool ReplicationClient::Vote(std::uint64_t term, const std::string& candidate_id, bool* granted,
                             std::uint64_t last_log_index, std::uint64_t last_log_term) {
  if (granted == nullptr || candidate_id.empty() || candidate_id.size() > UINT16_MAX) return false;
  // 构造投票请求载荷
  std::string payload;
  Put64(&payload, term);
  Put16(&payload, static_cast<std::uint16_t>(candidate_id.size()));
  payload.append(candidate_id);
  Put64(&payload, last_log_index);
  Put64(&payload, last_log_term);
  std::string response;
  if (!Call(static_cast<std::uint8_t>(protocol::Command::kReplicaVote), candidate_id,
            std::move(payload), &response))
    return false;
  if (response.size() != 1) {
    error_ = "invalid vote response";
    return false;
  }
  *granted = response[0] != 0;
  return true;
}

// 请求预投票
// 预投票不递增任期，用于检测是否有可能赢得选举
bool ReplicationClient::PreVote(std::uint64_t term, const std::string& candidate_id,
                                bool* granted, std::uint64_t last_log_index,
                                std::uint64_t last_log_term) {
  if (granted == nullptr || candidate_id.empty() || candidate_id.size() > UINT16_MAX) return false;
  // 构造预投票请求载荷
  std::string payload;
  Put64(&payload, term);
  Put16(&payload, static_cast<std::uint16_t>(candidate_id.size()));
  payload.append(candidate_id);
  Put64(&payload, last_log_index);
  Put64(&payload, last_log_term);
  std::string response;
  if (!Call(static_cast<std::uint8_t>(protocol::Command::kReplicaVote), candidate_id,
            std::move(payload), &response, true) || response.size() != 1)
    return false;
  *granted = response[0] != 0;
  return true;
}

}  // namespace mq::server