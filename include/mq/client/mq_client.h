#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "mq/core/storage_engine.h"

namespace mq::client {

enum class AckMode { kZero, kOne, kAll };
struct ProduceResult { std::uint32_t partition = 0; std::uint64_t offset = 0; };
struct ProducerMessage { std::string key; std::string value; };
struct TopicInfo { std::string name; std::uint32_t partitions = 0; };

class MqProducer {
 public:
  MqProducer();
  ~MqProducer();
  MqProducer(const MqProducer&) = delete;
  bool connect(const std::string& host, std::uint16_t port);
  bool connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints);
  bool createTopic(const std::string& name, std::uint32_t partitions = 1);
  bool produce(const std::string& topic, const std::string& key, const std::string& value);
  bool produce(const std::string& topic, const std::string& key, const std::string& value,
               AckMode ack, ProduceResult* result = nullptr);
  bool produceBatch(const std::string& topic, const std::vector<ProducerMessage>& messages,
                    AckMode ack = AckMode::kOne, std::vector<ProduceResult>* results = nullptr);
  bool flush();
  bool listTopics(std::vector<TopicInfo>* topics);
  bool metrics(std::string* output);
  void close();
  void setTimeoutMs(std::uint32_t timeout_ms);
  void setProducerId(std::uint64_t producer_id);
  const std::string& lastError() const;

 public:
  struct Impl;
 private:
  std::unique_ptr<Impl> impl_;
};

class MqConsumer {
 public:
  MqConsumer();
  ~MqConsumer();
  MqConsumer(const MqConsumer&) = delete;
  bool connect(const std::string& host, std::uint16_t port);
  bool connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints);
  bool subscribe(const std::string& topic, const std::string& group);
  bool subscribe(const std::string& topic, const std::string& group, std::uint32_t partition);
  std::optional<core::Message> poll(std::uint32_t timeout_ms = 5000);
  bool commit(std::uint64_t offset);
  void close();
  void setTimeoutMs(std::uint32_t timeout_ms);
  const std::string& lastError() const;

 public:
  struct Impl;
 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace mq::client
