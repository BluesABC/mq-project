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
void CloseSocket(Socket socket) {
#ifdef _WIN32
  closesocket(socket);
#else
  close(socket);
#endif
}
void Put16(std::string* out, std::uint16_t value) { out->push_back(static_cast<char>(value >> 8)); out->push_back(static_cast<char>(value)); }
void Put32(std::string* out, std::uint32_t value) { for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift)); }
void Put64(std::string* out, std::uint64_t value) { for (int shift = 56; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift)); }
std::uint16_t Get16(std::string_view data, std::size_t pos) { return (static_cast<std::uint16_t>(static_cast<unsigned char>(data[pos])) << 8) | static_cast<unsigned char>(data[pos + 1]); }
std::uint32_t Get32(std::string_view data, std::size_t pos) { std::uint32_t value = 0; for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]); return value; }
std::uint64_t Get64(std::string_view data, std::size_t pos) { std::uint64_t value = 0; for (int i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]); return value; }
bool SendAll(Socket socket, std::string_view data) { std::size_t sent = 0; while (sent < data.size()) { const int count = send(socket, data.data() + sent, static_cast<int>(data.size() - sent), 0); if (count <= 0) return false; sent += static_cast<std::size_t>(count); } return true; }
bool ReceiveAll(Socket socket, char* data, std::size_t size) { std::size_t received = 0; while (received < size) { const int count = recv(socket, data + received, static_cast<int>(size - received), 0); if (count <= 0) return false; received += static_cast<std::size_t>(count); } return true; }
void SetTimeout(Socket socket, std::uint32_t timeout_ms) {
#ifdef _WIN32
  const int value = static_cast<int>(timeout_ms); setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&value), sizeof(value)); setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&value), sizeof(value));
#else
  timeval value{}; value.tv_sec = timeout_ms / 1000; value.tv_usec = (timeout_ms % 1000) * 1000; setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value)); setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value));
#endif
}
}

ReplicationClient::ReplicationClient(std::string host, std::uint16_t port, std::uint32_t timeout_ms)
    : host_(std::move(host)), port_(port), timeout_ms_(timeout_ms == 0 ? 1000 : timeout_ms) {}

bool ReplicationClient::Call(std::uint8_t command, const std::string& topic, std::string payload,
                             std::string* response_payload) {
#ifdef _WIN32
  static bool initialized = false;
  if (!initialized) { WSADATA data{}; if (WSAStartup(MAKEWORD(2, 2), &data) != 0) { error_ = "WSAStartup failed"; return false; } initialized = true; }
#endif
  Socket socket = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socket == kInvalidSocket) { error_ = "socket failed"; return false; }
  addrinfo hints{}; hints.ai_family = AF_INET; hints.ai_socktype = SOCK_STREAM; addrinfo* result = nullptr;
  if (getaddrinfo(host_.c_str(), std::to_string(port_).c_str(), &hints, &result) != 0 || result == nullptr) { CloseSocket(socket); error_ = "resolve failed"; return false; }
  SetTimeout(socket, timeout_ms_);
  const bool connected = ::connect(socket, result->ai_addr, static_cast<int>(result->ai_addrlen)) == 0;
  freeaddrinfo(result);
  if (!connected) { CloseSocket(socket); error_ = "connect failed"; return false; }
  protocol::Request request; request.command = static_cast<protocol::Command>(command); request.request_id = request_id_++; request.flags = protocol::kFlagReplication; request.topic = topic; request.payload = std::move(payload);
  std::string frame;
  protocol::Response response;
  bool okay = protocol::ProtocolCodec::EncodeRequest(request, &frame) && SendAll(socket, frame);
  char header[18]{};
  if (okay && ReceiveAll(socket, header, sizeof(header))) {
    const auto payload_size = Get32(std::string_view(header, sizeof(header)), 14);
    if (payload_size <= protocol::kMaxPayloadBytes) {
      std::string full(header, sizeof(header)); full.resize(18 + payload_size);
      okay = ReceiveAll(socket, full.data() + 18, payload_size) && protocol::ProtocolCodec::DecodeResponse(full, &response, &error_);
    } else okay = false;
  } else okay = false;
  CloseSocket(socket);
  if (!okay) { if (error_.empty()) error_ = "replication response failed"; return false; }
  if (response.status != protocol::Status::kOk) { error_ = "replication request rejected"; return false; }
  if (response_payload != nullptr) *response_payload = std::move(response.payload);
  error_.clear();
  return true;
}

bool ReplicationClient::Fetch(const std::string& topic, std::uint32_t partition, std::uint64_t offset,
                              std::uint32_t max_bytes, std::vector<core::Message>* messages) {
  if (messages == nullptr || max_bytes == 0) return false;
  std::string payload; Put32(&payload, partition); Put64(&payload, offset); Put32(&payload, max_bytes);
  std::string response;
  if (!Call(static_cast<std::uint8_t>(protocol::Command::kReplicaFetch), topic, std::move(payload), &response) || response.size() < 4) return false;
  std::size_t position = 4; const auto count = Get32(response, 0); messages->clear(); messages->reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    if (position + 18 > response.size()) { error_ = "invalid replica fetch response"; return false; }
    core::Message message; message.offset = Get64(response, position); position += 8; message.timestamp_ms = static_cast<std::int64_t>(Get64(response, position)); position += 8;
    const auto key_size = Get16(response, position); position += 2; if (position + key_size + 4 > response.size()) return false;
    message.key.assign(response, position, key_size); position += key_size; const auto value_size = Get32(response, position); position += 4; if (position + value_size > response.size()) return false;
    message.value.assign(response, position, value_size); position += value_size; messages->push_back(std::move(message));
  }
  return position == response.size();
}

bool ReplicationClient::Append(const std::string& topic, std::uint32_t partition, const std::vector<core::Message>& messages) {
  if (messages.empty()) return false;
  std::string payload; Put32(&payload, partition); Put32(&payload, static_cast<std::uint32_t>(messages.size()));
  for (const auto& message : messages) { Put64(&payload, message.offset); Put64(&payload, static_cast<std::uint64_t>(message.timestamp_ms)); Put16(&payload, static_cast<std::uint16_t>(message.key.size())); payload.append(message.key); Put32(&payload, static_cast<std::uint32_t>(message.value.size())); payload.append(message.value); }
  return Call(static_cast<std::uint8_t>(protocol::Command::kReplicaAppend), topic, std::move(payload), nullptr);
}

bool ReplicationClient::Heartbeat(const std::string& node_id, std::uint64_t replicated_offset) {
  std::string payload; Put16(&payload, static_cast<std::uint16_t>(node_id.size())); payload.append(node_id); Put64(&payload, replicated_offset);
  return Call(static_cast<std::uint8_t>(protocol::Command::kHeartbeat), node_id, std::move(payload), nullptr);
}

}  // namespace mq::server
