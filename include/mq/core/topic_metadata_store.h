#pragma once

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "mq/core/queue_manager.h"

namespace mq::core {

// 元数据单独落盘，Broker 重启时可先恢复 Topic，再打开对应消息分区。
class TopicMetadataStore {
 public:
  explicit TopicMetadataStore(std::filesystem::path path) : path_(std::move(path)) {}
  bool Save(const std::vector<TopicMetadata>& topics, std::string* error = nullptr) const;
  bool Load(std::vector<TopicMetadata>* topics, std::string* error = nullptr) const;

 private:
  std::filesystem::path path_;
};

}  // namespace mq::core
