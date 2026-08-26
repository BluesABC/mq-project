#include "mq/client/mq_client.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <random>
#include <thread>

#include "mq/protocol/protocol_codec.h"
#include "mq/network/tls.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using Socket = SOCKET;
constexpr Socket kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
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
void Put16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}
void Put32(std::string* out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}
void Put64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}
std::uint16_t Get16(std::string_view data, std::size_t pos) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(data[pos])) << 8) |
         static_cast<unsigned char>(data[pos + 1]);
}
std::uint32_t Get32(std::string_view data, std::size_t pos) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}
std::uint64_t Get64(std::string_view data, std::size_t pos) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}
bool SendAll(Socket socket, std::string_view data) {
  std::size_t sent = 0;
  while (sent < data.size()) {
    const int count = send(socket, data.data() + sent, static_cast<int>(data.size() - sent), 0);
    if (count <= 0) return false;
    sent += static_cast<std::size_t>(count);
  }
  return true;
}
bool ReceiveAll(Socket socket, char* data, std::size_t size) {
  std::size_t received = 0;
  while (received < size) {
    const int count = recv(socket, data + received, static_cast<int>(size - received), 0);
    if (count <= 0) return false;
    received += static_cast<std::size_t>(count);
  }
  return true;
}
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

struct MqProducer::Impl {
  Socket socket = kInvalidSocket;
  std::string host;
  std::uint16_t port = 0;
  std::vector<std::pair<std::string, std::uint16_t>> endpoints;
  std::size_t endpoint_index = 0;
  std::uint32_t timeout_ms = 5000;
  std::uint64_t request_id = 1;
  std::uint64_t producer_id = 0;
  std::uint64_t sequence = 0;
  std::chrono::milliseconds backoff{100};
  std::string error;
  std::string auth_token;
  network::TlsOptions tls_options;
  std::unique_ptr<network::TlsSessionContext> tls_context;
  network::TlsHandle tls_session = nullptr;
  Impl() {
    static std::atomic<std::uint64_t> sequence{1};
    std::random_device random;
    const auto clock = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    producer_id = (static_cast<std::uint64_t>(random()) << 32) ^ random() ^ clock ^
                  sequence.fetch_add(1, std::memory_order_relaxed);
    if (producer_id == 0) producer_id = sequence.fetch_add(1, std::memory_order_relaxed);
  }
  ~Impl() {
    Close();
  }
  void Close() {
    if (tls_session != nullptr && tls_context != nullptr) {
      tls_context->CloseSession(tls_session);
      tls_session = nullptr;
    }
    if (socket != kInvalidSocket) {
      CloseSocket(socket);
      socket = kInvalidSocket;
    }
  }
  bool Connect() {
    if (socket != kInvalidSocket) return true;
    if (endpoints.empty() && !host.empty()) endpoints.emplace_back(host, port);
    if (endpoints.empty()) return false;
#ifdef _WIN32
    static bool initialized = false;
    if (!initialized) {
      WSADATA data{};
      if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return false;
      initialized = true;
    }
#endif
    // 端点按轮询顺序尝试，配合退避避免 Broker 故障时持续高频连接。
    for (std::size_t attempt = 0; attempt < endpoints.size(); ++attempt) {
      const auto index = (endpoint_index + attempt) % endpoints.size();
      host = endpoints[index].first;
      port = endpoints[index].second;
      socket = ::socket(AF_INET, SOCK_STREAM, 0);
      if (socket == kInvalidSocket) continue;
      addrinfo hints{};
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      addrinfo* result = nullptr;
      if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result) != 0 ||
          result == nullptr) {
        Close();
        continue;
      }
      SetTimeout(socket, timeout_ms);
      const bool connected =
          ::connect(socket, result->ai_addr, static_cast<int>(result->ai_addrlen)) == 0;
      freeaddrinfo(result);
      if (connected) {
        if (tls_options.enabled) {
          if (!tls_context)
            tls_context = network::TlsSessionContext::CreateClient(tls_options, &error);
          if (!tls_context) {
            Close();
            continue;
          }
          tls_session = tls_context->NewSession(static_cast<int>(socket), false, &error);
          if (tls_session == nullptr) {
            Close();
            continue;
          }
          for (;;) {
            const auto handshake = tls_context->Handshake(tls_session, &error);
            if (handshake == network::TlsIoResult::kOk) break;
            if (handshake != network::TlsIoResult::kWantRead &&
                handshake != network::TlsIoResult::kWantWrite) {
              Close();
              break;
            }
          }
          if (tls_session == nullptr) continue;
        }
        endpoint_index = index;
        return true;
      }
      Close();
    }
    endpoint_index = (endpoint_index + 1) % endpoints.size();
    return false;
  }
  bool SendAllData(std::string_view data) {
    if (tls_session == nullptr) return SendAll(socket, data);
    std::size_t sent = 0;
    while (sent < data.size()) {
      std::size_t written = 0;
      std::string tls_error;
      const auto result = tls_context->Write(tls_session, data.data() + sent, data.size() - sent,
                                             &written, &tls_error);
      if (result != network::TlsIoResult::kOk || written == 0) {
        error = tls_error.empty() ? "TLS send failed" : tls_error;
        return false;
      }
      sent += written;
    }
    return true;
  }
  bool ReceiveAllData(char* data, std::size_t size) {
    if (tls_session == nullptr) return ReceiveAll(socket, data, size);
    std::size_t received = 0;
    while (received < size) {
      std::size_t read = 0;
      std::string tls_error;
      const auto result = tls_context->Read(tls_session, data + received, size - received, &read,
                                            &tls_error);
      if (result != network::TlsIoResult::kOk || read == 0) {
        error = tls_error.empty() ? "TLS receive failed" : tls_error;
        return false;
      }
      received += read;
    }
    return true;
  }
  bool Call(mq::protocol::Request request, mq::protocol::Response* response) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    // 每次重试都重新建立连接，但 deadline 不重置，保证调用方的超时有明确上界。
    if (!auth_token.empty()) {
      if (auth_token.size() > 65535) {
        error = "auth token exceeds protocol limit";
        return false;
      }
      std::string payload;
      Put16(&payload, static_cast<std::uint16_t>(auth_token.size()));
      payload.append(auth_token);
      payload.append(request.payload);
      request.payload = std::move(payload);
      request.flags |= protocol::kFlagAuthentication;
    }
    while (std::chrono::steady_clock::now() < deadline) {
      if (!Connect()) {
        std::this_thread::sleep_for(backoff);
        const auto doubled = backoff * 2;
        backoff =
            doubled < std::chrono::milliseconds(30000) ? doubled : std::chrono::milliseconds(30000);
        continue;
      }
      request.request_id = request_id++;
      std::string frame;
      if (mq::protocol::ProtocolCodec::EncodeRequest(request, &frame) && SendAllData(frame)) {
        char header[18];
        if (ReceiveAllData(header, sizeof(header))) {
          const auto payload_size = Get32(std::string_view(header, sizeof(header)), 14);
          if (payload_size > mq::protocol::kMaxPayloadBytes) {
            error = "response payload too large";
            Close();
            continue;
          }
          std::string full(header, sizeof(header));
          full.resize(18 + payload_size);
          if (ReceiveAllData(full.data() + 18, payload_size) &&
              mq::protocol::ProtocolCodec::DecodeResponse(full, response, &error)) {
            if (response->request_id != request.request_id) {
              error = "response request_id mismatch";
              Close();
              continue;
            }
            if (response->status == mq::protocol::Status::kNotLeader && endpoints.size() > 1) {
              Close();
              endpoint_index = (endpoint_index + 1) % endpoints.size();
              continue;
            }
            backoff = std::chrono::milliseconds(100);
            return true;
          }
        }
      }
      Close();
    }
    error = "request timeout";
    return false;
  }
  bool SendWithoutResponse(mq::protocol::Request request) {
    if (!auth_token.empty()) {
      if (auth_token.size() > 65535) {
        error = "auth token exceeds protocol limit";
        return false;
      }
      std::string payload;
      Put16(&payload, static_cast<std::uint16_t>(auth_token.size()));
      payload.append(auth_token);
      payload.append(request.payload);
      request.payload = std::move(payload);
      request.flags |= protocol::kFlagAuthentication;
    }
    if (!Connect()) return false;
    request.request_id = request_id++;
    std::string frame;
    const bool sent = mq::protocol::ProtocolCodec::EncodeRequest(request, &frame) &&
                      SendAllData(frame);
    Close();
    return sent;
  }
};

MqProducer::MqProducer() : impl_(std::make_unique<Impl>()) {}
MqProducer::~MqProducer() = default;
bool MqProducer::connect(const std::string& host, std::uint16_t port) {
  impl_->host = host;
  impl_->port = port;
  impl_->Close();
  return impl_->Connect();
}
bool MqProducer::connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints) {
  if (endpoints.empty()) return false;
  impl_->endpoints = endpoints;
  impl_->endpoint_index = 0;
  impl_->Close();
  return impl_->Connect();
}
bool MqProducer::createTopic(const std::string& name, std::uint32_t partitions) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kCreateTopic;
  request.topic = name;
  Put32(&request.payload, partitions);
  mq::protocol::Response response;
  return impl_->Call(request, &response) && response.status == mq::protocol::Status::kOk;
}
bool MqProducer::produce(const std::string& topic, const std::string& key,
                         const std::string& value) {
  return produce(topic, key, value, AckMode::kOne, nullptr);
}
bool MqProducer::produce(const std::string& topic, const std::string& key, const std::string& value,
                         AckMode ack, ProduceResult* result) {
  if (key.size() > 65535 || value.empty() || value.size() > 1024 * 1024 ||
      impl_->sequence == UINT64_MAX) {
    impl_->error = "message or sequence exceeds limit";
    return false;
  }
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kProduce;
  request.topic = topic;
  request.flags = protocol::kFlagProducerMetadata | static_cast<std::uint16_t>(ack);
  Put64(&request.payload, impl_->producer_id);
  Put64(&request.payload, impl_->sequence++);
  Put32(&request.payload, 0xFFFFFFFFu);
  Put16(&request.payload, static_cast<std::uint16_t>(key.size()));
  request.payload.append(key);
  Put32(&request.payload, static_cast<std::uint32_t>(value.size()));
  request.payload.append(value);
  if (ack == AckMode::kZero) return impl_->SendWithoutResponse(std::move(request));
  mq::protocol::Response response;
  if (!impl_->Call(std::move(request), &response) || response.status != mq::protocol::Status::kOk)
    return false;
  if (result && response.payload.size() == 12) {
    result->partition = Get32(response.payload, 0);
    result->offset = Get64(response.payload, 4);
  }
  return true;
}
bool MqProducer::produceBatch(const std::string& topic,
                              const std::vector<ProducerMessage>& messages, AckMode ack,
                              std::vector<ProduceResult>* results) {
  if (messages.empty() || messages.size() > 10000 ||
      impl_->sequence > UINT64_MAX - messages.size()) {
    impl_->error = "batch or sequence exceeds limit";
    return false;
  }
  for (const auto& message : messages) {
    if (message.key.size() > 65535 || message.value.empty() || message.value.size() > 1024 * 1024) {
      impl_->error = "message exceeds limit";
      return false;
    }
  }
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kProduceBatch;
  request.topic = topic;
  request.flags = protocol::kFlagProducerMetadata | static_cast<std::uint16_t>(ack);
  Put64(&request.payload, impl_->producer_id);
  Put64(&request.payload, impl_->sequence);
  Put32(&request.payload, static_cast<std::uint32_t>(messages.size()));
  for (const auto& message : messages) {
    Put16(&request.payload, static_cast<std::uint16_t>(message.key.size()));
    request.payload.append(message.key);
    Put32(&request.payload, static_cast<std::uint32_t>(message.value.size()));
    request.payload.append(message.value);
  }
  impl_->sequence += messages.size();
  if (ack == AckMode::kZero) return impl_->SendWithoutResponse(std::move(request));
  mq::protocol::Response response;
  if (!impl_->Call(std::move(request), &response) || response.status != mq::protocol::Status::kOk ||
      response.payload.size() < 4 || Get32(response.payload, 0) != messages.size())
    return false;
  if (results) {
    results->clear();
    std::size_t pos = 4;
    for (std::size_t i = 0; i < messages.size(); ++i) {
      if (pos + 12 > response.payload.size()) return false;
      results->push_back({Get32(response.payload, pos), Get64(response.payload, pos + 4)});
      pos += 12;
    }
  }
  return true;
}
bool MqProducer::flush() {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kHeartbeat;
  mq::protocol::Response response;
  return impl_->Call(request, &response) && response.status == mq::protocol::Status::kOk;
}
bool MqProducer::listTopics(std::vector<TopicInfo>* topics) {
  if (topics == nullptr) return false;
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kListTopic;
  mq::protocol::Response response;
  if (!impl_->Call(request, &response) || response.status != mq::protocol::Status::kOk ||
      response.payload.size() < 4)
    return false;
  const auto count = Get32(response.payload, 0);
  if (count > 100000) {
    impl_->error = "too many topics";
    return false;
  }
  topics->clear();
  topics->reserve(count);
  std::size_t position = 4;
  for (std::uint32_t index = 0; index < count; ++index) {
    if (position + 2 > response.payload.size()) {
      impl_->error = "invalid topic response";
      return false;
    }
    const auto name_size = Get16(response.payload, position);
    position += 2;
    if (position + name_size + 4 > response.payload.size()) {
      impl_->error = "invalid topic response";
      return false;
    }
    TopicInfo topic;
    topic.name.assign(response.payload, position, name_size);
    position += name_size;
    topic.partitions = Get32(response.payload, position);
    position += 4;
    topics->push_back(std::move(topic));
  }
  if (position != response.payload.size()) {
    impl_->error = "invalid topic response";
    return false;
  }
  return true;
}
bool MqProducer::metrics(std::string* output) {
  if (output == nullptr) return false;
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kMetrics;
  mq::protocol::Response response;
  if (!impl_->Call(request, &response) || response.status != mq::protocol::Status::kOk)
    return false;
  *output = std::move(response.payload);
  return true;
}
void MqProducer::close() {
  impl_->Close();
}
void MqProducer::setTimeoutMs(std::uint32_t timeout_ms) {
  impl_->timeout_ms = timeout_ms == 0 ? 5000 : timeout_ms;
}
void MqProducer::setAuthToken(std::string token) {
  if (token.size() > 65535) {
    impl_->error = "auth token exceeds protocol limit";
    return;
  }
  impl_->auth_token = std::move(token);
}
void MqProducer::setTlsOptions(network::TlsOptions options) {
  impl_->tls_options = std::move(options);
  impl_->Close();
  impl_->tls_context.reset();
}
void MqProducer::setProducerId(std::uint64_t producer_id) {
  impl_->producer_id = producer_id;
}
const std::string& MqProducer::lastError() const {
  return impl_->error;
}

struct MqConsumer::Impl : MqProducer::Impl {
  std::string topic, group;
  std::uint32_t partition = 0;
  std::uint64_t next_offset = 0;
  std::deque<core::Message> pending_messages;
};
MqConsumer::MqConsumer() : impl_(std::make_unique<Impl>()) {}
MqConsumer::~MqConsumer() = default;
bool MqConsumer::connect(const std::string& host, std::uint16_t port) {
  impl_->host = host;
  impl_->port = port;
  impl_->pending_messages.clear();
  impl_->Close();
  return impl_->Connect();
}
bool MqConsumer::connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints) {
  if (endpoints.empty()) return false;
  impl_->endpoints = endpoints;
  impl_->endpoint_index = 0;
  impl_->pending_messages.clear();
  impl_->Close();
  return impl_->Connect();
}
bool MqConsumer::subscribe(const std::string& topic, const std::string& group) {
  return subscribe(topic, group, 0);
}
bool MqConsumer::subscribe(const std::string& topic, const std::string& group,
                           std::uint32_t partition) {
  impl_->topic = topic;
  impl_->group = group;
  impl_->partition = partition;
  impl_->next_offset = 0;
  impl_->pending_messages.clear();
  return !topic.empty() && !group.empty();
}
std::optional<core::Message> MqConsumer::poll(std::uint32_t timeout_ms) {
  // 先消费已拉取的批次，减少网络往返；只有本地缓存为空时才发送 Fetch。
  if (!impl_->pending_messages.empty()) {
    auto message = std::move(impl_->pending_messages.front());
    impl_->pending_messages.pop_front();
    impl_->next_offset = message.offset + 1;
    return message;
  }
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kFetch;
  request.topic = impl_->topic;
  Put32(&request.payload, impl_->partition);
  Put16(&request.payload, static_cast<std::uint16_t>(impl_->group.size()));
  request.payload.append(impl_->group);
  Put64(&request.payload, impl_->next_offset);
  Put32(&request.payload, 1024 * 1024);
  const auto old = impl_->timeout_ms;
  impl_->timeout_ms = timeout_ms == 0 ? old : timeout_ms;
  mq::protocol::Response response;
  const bool okay = impl_->Call(request, &response);
  impl_->timeout_ms = old;
  if (!okay) return std::nullopt;
  if (response.status != mq::protocol::Status::kOk) {
    impl_->error =
        "fetch failed with status " + std::to_string(static_cast<unsigned>(response.status));
    return std::nullopt;
  }
  if (response.payload.size() < 4 || Get32(response.payload, 0) == 0) return std::nullopt;
  const auto message_count = Get32(response.payload, 0);
  if (message_count > 100000) {
    impl_->error = "too many messages in fetch response";
    return std::nullopt;
  }
  std::size_t pos = 4;
  for (std::uint32_t index = 0; index < message_count; ++index) {
    if (pos + 18 > response.payload.size()) {
      impl_->error = "invalid fetch response header";
      impl_->pending_messages.clear();
      return std::nullopt;
    }
    core::Message message;
    message.offset = Get64(response.payload, pos);
    message.timestamp_ms = static_cast<std::int64_t>(Get64(response.payload, pos + 8));
    pos += 16;
    const auto key_size = Get16(response.payload, pos);
    pos += 2;
    if (pos + key_size + 4 > response.payload.size()) {
      impl_->error = "invalid fetch response key";
      impl_->pending_messages.clear();
      return std::nullopt;
    }
    message.key.assign(response.payload, pos, key_size);
    pos += key_size;
    const auto value_size = Get32(response.payload, pos);
    pos += 4;
    if (pos + value_size > response.payload.size()) {
      impl_->error = "invalid fetch response value";
      impl_->pending_messages.clear();
      return std::nullopt;
    }
    message.value.assign(response.payload, pos, value_size);
    pos += value_size;
    impl_->pending_messages.push_back(std::move(message));
  }
  if (pos != response.payload.size() || impl_->pending_messages.empty()) {
    impl_->error = "invalid fetch response length";
    impl_->pending_messages.clear();
    return std::nullopt;
  }
  auto message = std::move(impl_->pending_messages.front());
  impl_->pending_messages.pop_front();
  impl_->next_offset = message.offset + 1;
  return message;
}
bool MqConsumer::commit(std::uint64_t offset) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kCommitOffset;
  request.topic = impl_->topic;
  Put16(&request.payload, static_cast<std::uint16_t>(impl_->group.size()));
  request.payload.append(impl_->group);
  Put32(&request.payload, impl_->partition);
  Put64(&request.payload, offset);
  mq::protocol::Response response;
  return impl_->Call(request, &response) && response.status == mq::protocol::Status::kOk;
}
void MqConsumer::close() {
  impl_->pending_messages.clear();
  impl_->Close();
}
void MqConsumer::setTimeoutMs(std::uint32_t timeout_ms) {
  impl_->timeout_ms = timeout_ms == 0 ? 5000 : timeout_ms;
}
void MqConsumer::setAuthToken(std::string token) {
  if (token.size() > 65535) {
    impl_->error = "auth token exceeds protocol limit";
    return;
  }
  impl_->auth_token = std::move(token);
}
void MqConsumer::setTlsOptions(network::TlsOptions options) {
  impl_->tls_options = std::move(options);
  impl_->Close();
  impl_->tls_context.reset();
}
const std::string& MqConsumer::lastError() const {
  return impl_->error;
}
}  // namespace mq::client
