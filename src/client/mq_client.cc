#include "mq/client/mq_client.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <random>
#include <thread>

#include "mq/protocol/protocol_codec.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using Socket = SOCKET;
constexpr Socket kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
using Socket = int;
constexpr Socket kInvalidSocket = -1;
#endif

namespace mq::client {
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

struct MqProducer::Impl {
  Socket socket = kInvalidSocket; std::string host; std::uint16_t port = 0; std::vector<std::pair<std::string, std::uint16_t>> endpoints; std::size_t endpoint_index = 0; std::uint32_t timeout_ms = 5000; std::uint64_t request_id = 1; std::uint64_t producer_id = 0; std::uint64_t sequence = 0; std::chrono::milliseconds backoff{100}; std::string error;
  Impl() { producer_id = static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()); }
  ~Impl() { Close(); }
  void Close() { if (socket != kInvalidSocket) { CloseSocket(socket); socket = kInvalidSocket; } }
  bool Connect() {
    if (socket != kInvalidSocket) return true;
    if (endpoints.empty() && !host.empty()) endpoints.emplace_back(host, port);
    if (endpoints.empty()) return false;
#ifdef _WIN32
    static bool initialized = false; if (!initialized) { WSADATA data{}; if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return false; initialized = true; }
#endif
    for (std::size_t attempt = 0; attempt < endpoints.size(); ++attempt) {
      const auto index = (endpoint_index + attempt) % endpoints.size(); host = endpoints[index].first; port = endpoints[index].second;
      socket = ::socket(AF_INET, SOCK_STREAM, 0); if (socket == kInvalidSocket) continue;
      addrinfo hints{}; hints.ai_family = AF_INET; hints.ai_socktype = SOCK_STREAM; addrinfo* result = nullptr;
      if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result) != 0 || result == nullptr) { Close(); continue; }
      SetTimeout(socket, timeout_ms); const bool connected = ::connect(socket, result->ai_addr, static_cast<int>(result->ai_addrlen)) == 0; freeaddrinfo(result);
      if (connected) { endpoint_index = index; return true; } Close();
    }
    endpoint_index = (endpoint_index + 1) % endpoints.size(); return false;
  }
  bool Call(mq::protocol::Request request, mq::protocol::Response* response) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      if (!Connect()) { std::this_thread::sleep_for(backoff); const auto doubled = backoff * 2; backoff = doubled < std::chrono::milliseconds(30000) ? doubled : std::chrono::milliseconds(30000); continue; }
      request.request_id = request_id++;
      std::string frame; if (mq::protocol::ProtocolCodec::EncodeRequest(request, &frame) && SendAll(socket, frame)) {
        char header[18]; if (ReceiveAll(socket, header, sizeof(header))) {
          const auto payload_size = Get32(std::string_view(header, sizeof(header)), 14);
          if (payload_size > mq::protocol::kMaxPayloadBytes) { error = "response payload too large"; Close(); continue; }
          std::string full(header, sizeof(header)); full.resize(18 + payload_size);
          if (ReceiveAll(socket, full.data() + 18, payload_size) && mq::protocol::ProtocolCodec::DecodeResponse(full, response, &error)) {
            if (response->status == mq::protocol::Status::kNotLeader && endpoints.size() > 1) {
              Close(); endpoint_index = (endpoint_index + 1) % endpoints.size(); continue;
            }
            backoff = std::chrono::milliseconds(100); return true;
          }
        }
      }
      Close();
    }
    error = "request timeout"; return false;
  }
  bool SendWithoutResponse(mq::protocol::Request request) {
    if (!Connect()) return false; request.request_id = request_id++; std::string frame; const bool sent = mq::protocol::ProtocolCodec::EncodeRequest(request, &frame) && SendAll(socket, frame); Close(); return sent;
  }
};

MqProducer::MqProducer() : impl_(std::make_unique<Impl>()) {}
MqProducer::~MqProducer() = default;
bool MqProducer::connect(const std::string& host, std::uint16_t port) { impl_->host = host; impl_->port = port; impl_->Close(); return impl_->Connect(); }
bool MqProducer::connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints) { if (endpoints.empty()) return false; impl_->endpoints = endpoints; impl_->endpoint_index = 0; impl_->Close(); return impl_->Connect(); }
bool MqProducer::createTopic(const std::string& name, std::uint32_t partitions) { mq::protocol::Request request; request.command = mq::protocol::Command::kCreateTopic; request.topic = name; Put32(&request.payload, partitions); mq::protocol::Response response; return impl_->Call(request, &response) && response.status == mq::protocol::Status::kOk; }
bool MqProducer::produce(const std::string& topic, const std::string& key, const std::string& value) { return produce(topic, key, value, AckMode::kOne, nullptr); }
bool MqProducer::produce(const std::string& topic, const std::string& key, const std::string& value, AckMode ack, ProduceResult* result) {
  mq::protocol::Request request; request.command = mq::protocol::Command::kProduce; request.topic = topic; request.flags = protocol::kFlagProducerMetadata | static_cast<std::uint16_t>(ack); Put64(&request.payload, impl_->producer_id); Put64(&request.payload, impl_->sequence++); Put32(&request.payload, 0xFFFFFFFFu); Put16(&request.payload, static_cast<std::uint16_t>(key.size())); request.payload.append(key); Put32(&request.payload, static_cast<std::uint32_t>(value.size())); request.payload.append(value);
  if (ack == AckMode::kZero) return impl_->SendWithoutResponse(std::move(request));
  mq::protocol::Response response; if (!impl_->Call(std::move(request), &response) || response.status != mq::protocol::Status::kOk) return false;
  if (result && response.payload.size() == 12) { result->partition = Get32(response.payload, 0); result->offset = Get64(response.payload, 4); } return true;
}
bool MqProducer::produceBatch(const std::string& topic, const std::vector<ProducerMessage>& messages, AckMode ack, std::vector<ProduceResult>* results) {
  mq::protocol::Request request; request.command = mq::protocol::Command::kProduceBatch; request.topic = topic; request.flags = protocol::kFlagProducerMetadata | static_cast<std::uint16_t>(ack); Put64(&request.payload, impl_->producer_id); Put64(&request.payload, impl_->sequence); Put32(&request.payload, static_cast<std::uint32_t>(messages.size())); for (const auto& message : messages) { Put16(&request.payload, static_cast<std::uint16_t>(message.key.size())); request.payload.append(message.key); Put32(&request.payload, static_cast<std::uint32_t>(message.value.size())); request.payload.append(message.value); } impl_->sequence += messages.size();
  if (ack == AckMode::kZero) return impl_->SendWithoutResponse(std::move(request));
  mq::protocol::Response response; if (!impl_->Call(std::move(request), &response) || response.status != mq::protocol::Status::kOk || response.payload.size() < 4 || Get32(response.payload, 0) != messages.size()) return false;
  if (results) { results->clear(); std::size_t pos = 4; for (std::size_t i = 0; i < messages.size(); ++i) { if (pos + 12 > response.payload.size()) return false; results->push_back({Get32(response.payload, pos), Get64(response.payload, pos + 4)}); pos += 12; } } return true;
}
bool MqProducer::flush() { mq::protocol::Request request; request.command = mq::protocol::Command::kHeartbeat; mq::protocol::Response response; return impl_->Call(request, &response) && response.status == mq::protocol::Status::kOk; }
void MqProducer::close() { impl_->Close(); }
void MqProducer::setTimeoutMs(std::uint32_t timeout_ms) { impl_->timeout_ms = timeout_ms == 0 ? 5000 : timeout_ms; }
void MqProducer::setProducerId(std::uint64_t producer_id) { impl_->producer_id = producer_id; }
const std::string& MqProducer::lastError() const { return impl_->error; }

struct MqConsumer::Impl : MqProducer::Impl {
  std::string topic, group; std::uint32_t partition = 0; std::uint64_t next_offset = 0;
};
MqConsumer::MqConsumer() : impl_(std::make_unique<Impl>()) {}
MqConsumer::~MqConsumer() = default;
bool MqConsumer::connect(const std::string& host, std::uint16_t port) { impl_->host = host; impl_->port = port; impl_->Close(); return impl_->Connect(); }
bool MqConsumer::connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints) { if (endpoints.empty()) return false; impl_->endpoints = endpoints; impl_->endpoint_index = 0; impl_->Close(); return impl_->Connect(); }
bool MqConsumer::subscribe(const std::string& topic, const std::string& group) { return subscribe(topic, group, 0); }
bool MqConsumer::subscribe(const std::string& topic, const std::string& group, std::uint32_t partition) { impl_->topic = topic; impl_->group = group; impl_->partition = partition; impl_->next_offset = 0; return !topic.empty() && !group.empty(); }
std::optional<core::Message> MqConsumer::poll(std::uint32_t timeout_ms) { mq::protocol::Request request; request.command = mq::protocol::Command::kFetch; request.topic = impl_->topic; Put32(&request.payload, impl_->partition); Put64(&request.payload, impl_->next_offset); Put32(&request.payload, 1024 * 1024); const auto old = impl_->timeout_ms; impl_->timeout_ms = timeout_ms == 0 ? old : timeout_ms; mq::protocol::Response response; const bool okay = impl_->Call(request, &response); impl_->timeout_ms = old; if (!okay || response.status != mq::protocol::Status::kOk || response.payload.size() < 4 || Get32(response.payload, 0) == 0) return std::nullopt; std::size_t pos = 4; if (pos + 18 > response.payload.size()) return std::nullopt; core::Message message; message.offset = Get64(response.payload, pos); message.timestamp_ms = static_cast<std::int64_t>(Get64(response.payload, pos + 8)); pos += 16; const auto key_size = Get16(response.payload, pos); pos += 2; if (pos + key_size + 4 > response.payload.size()) return std::nullopt; message.key.assign(response.payload, pos, key_size); pos += key_size; const auto value_size = Get32(response.payload, pos); pos += 4; if (pos + value_size > response.payload.size()) return std::nullopt; message.value.assign(response.payload, pos, value_size); impl_->next_offset = message.offset + 1; return message; }
bool MqConsumer::commit(std::uint64_t offset) { mq::protocol::Request request; request.command = mq::protocol::Command::kCommitOffset; request.topic = impl_->topic; Put16(&request.payload, static_cast<std::uint16_t>(impl_->group.size())); request.payload.append(impl_->group); Put32(&request.payload, impl_->partition); Put64(&request.payload, offset); mq::protocol::Response response; return impl_->Call(request, &response) && response.status == mq::protocol::Status::kOk; }
void MqConsumer::close() { impl_->Close(); }
void MqConsumer::setTimeoutMs(std::uint32_t timeout_ms) { impl_->timeout_ms = timeout_ms == 0 ? 5000 : timeout_ms; }
const std::string& MqConsumer::lastError() const { return impl_->error; }
}  // namespace mq::client
