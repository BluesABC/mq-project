#include "mq/server/broker.h"

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "mq/core/logger.h"

namespace mq::server {
namespace {

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

bool Take(std::string_view input, std::size_t* position, std::size_t count) {
  if (position == nullptr || *position > input.size() || count > input.size() - *position) return false;
  *position += count;
  return true;
}

std::uint16_t Get16(std::string_view input, std::size_t position) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(input[position])) << 8) |
         static_cast<unsigned char>(input[position + 1]);
}

std::uint32_t Get32(std::string_view input, std::size_t position) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}

std::uint64_t Get64(std::string_view input, std::size_t position) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}

}  // namespace

Broker::Broker(std::filesystem::path data_dir)
    : storage_(data_dir), metadata_store_(data_dir / "metadata" / "topics.meta") {}

bool Broker::Open(std::string* error) {
  if (opened_) return true;
  if (!storage_.Open(error)) {
    core::Logger::Instance().Log(core::LogLevel::kCritical, "broker storage initialization failed");
    return false;
  }
  std::vector<core::TopicMetadata> topics;
  if (!metadata_store_.Load(&topics, error) || !queues_.ReplaceTopics(std::move(topics), error)) return false;
  opened_ = true;
  core::Logger::Instance().Log(core::LogLevel::kInfo, "broker storage initialized");
  return true;
}

protocol::Response Broker::Handle(const protocol::Request& request) {
  if (!opened_) {
    core::Logger::Instance().Log(core::LogLevel::kError, "broker request received before open");
    return MakeResponse(request, protocol::Status::kInternalError);
  }
  switch (request.command) {
    case protocol::Command::kCreateTopic:
      return HandleCreateTopic(request);
    case protocol::Command::kListTopic:
      return HandleListTopic(request);
    case protocol::Command::kDeleteTopic:
      return HandleDeleteTopic(request);
    case protocol::Command::kProduce:
      return HandleProduce(request);
    case protocol::Command::kFetch:
      return HandleFetch(request);
    default:
      return MakeResponse(request, protocol::Status::kBadRequest);
  }
}

protocol::Response Broker::HandleCreateTopic(const protocol::Request& request) {
  if (request.payload.size() != 4) return MakeResponse(request, protocol::Status::kBadRequest);
  std::lock_guard<std::mutex> lock(topic_metadata_mutex_);
  std::string error;
  if (queues_.CreateTopic(request.topic, Get32(request.payload, 0), &error)) {
    if (!metadata_store_.Save(queues_.ListTopics(), &error)) {
      queues_.DeleteTopic(request.topic, nullptr);
      return MakeResponse(request, protocol::Status::kStorageError);
    }
    return MakeResponse(request, protocol::Status::kOk);
  }
  return MakeResponse(request, error == "topic already exists" ? protocol::Status::kTopicExists
                                                                : protocol::Status::kBadRequest);
}

protocol::Response Broker::HandleDeleteTopic(const protocol::Request& request) {
  if (!request.payload.empty()) return MakeResponse(request, protocol::Status::kBadRequest);
  std::lock_guard<std::mutex> lock(topic_metadata_mutex_);
  std::string error;
  core::TopicMetadata deleted_topic;
  if (!queues_.GetTopic(request.topic, &deleted_topic)) {
    return MakeResponse(request, protocol::Status::kUnknownTopic);
  }
  if (!queues_.DeleteTopic(request.topic, &error)) return MakeResponse(request, protocol::Status::kUnknownTopic);
  if (!metadata_store_.Save(queues_.ListTopics(), &error)) {
    queues_.CreateTopic(std::move(deleted_topic.name), deleted_topic.partition_count, nullptr);
    return MakeResponse(request, protocol::Status::kStorageError);
  }
  return MakeResponse(request, protocol::Status::kOk);
}

protocol::Response Broker::HandleListTopic(const protocol::Request& request) {
  if (!request.topic.empty() || !request.payload.empty()) {
    return MakeResponse(request, protocol::Status::kBadRequest);
  }
  const auto topics = queues_.ListTopics();
  std::string payload;
  Put32(&payload, static_cast<std::uint32_t>(topics.size()));
  for (const auto& topic : topics) {
    Put16(&payload, static_cast<std::uint16_t>(topic.name.size()));
    payload.append(topic.name);
    Put32(&payload, topic.partition_count);
  }
  return MakeResponse(request, protocol::Status::kOk, std::move(payload));
}

protocol::Response Broker::HandleProduce(const protocol::Request& request) {
  std::string_view payload = request.payload;
  std::size_t position = 0;
  if (!Take(payload, &position, 4)) return MakeResponse(request, protocol::Status::kBadRequest);
  const std::uint32_t requested_partition = Get32(payload, 0);
  if (!Take(payload, &position, 2)) return MakeResponse(request, protocol::Status::kBadRequest);
  const std::uint16_t key_length = Get16(payload, 4);
  const std::size_t key_position = position;
  if (!Take(payload, &position, key_length) || !Take(payload, &position, 4)) {
    return MakeResponse(request, protocol::Status::kBadRequest);
  }
  const std::uint32_t value_length = Get32(payload, position - 4);
  const std::size_t value_position = position;
  if (!Take(payload, &position, value_length) || position != payload.size()) {
    return MakeResponse(request, protocol::Status::kBadRequest);
  }
  std::uint32_t partition = 0;
  std::string error;
  const std::string key(payload.substr(key_position, key_length));
  if (!queues_.ResolvePartition(request.topic, requested_partition, key, &partition, &error)) {
    return MakeResponse(request, error == "unknown topic" ? protocol::Status::kUnknownTopic
                                                           : protocol::Status::kBadRequest);
  }
  core::Message message;
  if (!storage_.Append(request.topic, partition, key, std::string(payload.substr(value_position, value_length)),
                       &message, &error)) {
    return MakeResponse(request, protocol::Status::kStorageError);
  }
  std::string response_payload;
  Put32(&response_payload, partition);
  Put64(&response_payload, message.offset);
  return MakeResponse(request, protocol::Status::kOk, std::move(response_payload));
}

protocol::Response Broker::HandleFetch(const protocol::Request& request) {
  if (request.payload.size() != 16) return MakeResponse(request, protocol::Status::kBadRequest);
  const std::uint32_t partition = Get32(request.payload, 0);
  const std::uint64_t offset = Get64(request.payload, 4);
  const std::uint32_t max_bytes = Get32(request.payload, 12);
  if (max_bytes == 0) return MakeResponse(request, protocol::Status::kBadRequest);
  std::uint32_t resolved_partition = 0;
  std::string error;
  if (!queues_.ResolvePartition(request.topic, partition, "", &resolved_partition, &error)) {
    return MakeResponse(request, error == "unknown topic" ? protocol::Status::kUnknownTopic
                                                           : protocol::Status::kInvalidOffset);
  }
  std::vector<core::Message> messages;
  if (!storage_.Read(request.topic, resolved_partition, offset, max_bytes, &messages, &error)) {
    return MakeResponse(request, protocol::Status::kStorageError);
  }
  std::string response_payload;
  Put32(&response_payload, static_cast<std::uint32_t>(messages.size()));
  for (const auto& message : messages) {
    Put64(&response_payload, message.offset);
    Put64(&response_payload, static_cast<std::uint64_t>(message.timestamp_ms));
    Put16(&response_payload, static_cast<std::uint16_t>(message.key.size()));
    response_payload.append(message.key);
    Put32(&response_payload, static_cast<std::uint32_t>(message.value.size()));
    response_payload.append(message.value);
  }
  return MakeResponse(request, protocol::Status::kOk, std::move(response_payload));
}

protocol::Response Broker::MakeResponse(const protocol::Request& request, protocol::Status status,
                                        std::string payload) const {
  protocol::Response response;
  response.status = status;
  response.request_id = request.request_id;
  response.payload = std::move(payload);
  return response;
}

}  // namespace mq::server
