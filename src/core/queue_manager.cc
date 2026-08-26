#include "mq/core/queue_manager.h"

#include <algorithm>
#include <mutex>
#include <utility>

#include "mq/core/topic.h"

namespace mq::core {
namespace {

constexpr std::uint32_t kMaxPartitions = 1024;

std::uint64_t HashKey(const std::string& key) {
  std::uint64_t hash = 14695981039346656037ull;
  for (const unsigned char byte : key) {
    hash ^= byte;
    hash *= 1099511628211ull;
  }
  return hash;
}

}  // namespace

bool QueueManager::CreateTopic(std::string name, std::uint32_t partition_count,
                               std::string* error) {
  if (!IsValidTopicName(name) || partition_count == 0 || partition_count > kMaxPartitions) {
    if (error != nullptr) *error = "invalid topic metadata";
    return false;
  }
  std::unique_lock lock(mutex_);
  TopicMetadata metadata{name, partition_count};
  const auto result = topics_.emplace(std::move(name), std::move(metadata));
  if (!result.second && error != nullptr) *error = "topic already exists";
  return result.second;
}

bool QueueManager::DeleteTopic(const std::string& name, std::string* error) {
  std::unique_lock lock(mutex_);
  if (topics_.erase(name) == 0) {
    if (error != nullptr) *error = "unknown topic";
    return false;
  }
  return true;
}

bool QueueManager::ReplaceTopics(std::vector<TopicMetadata> topics, std::string* error) {
  std::unordered_map<std::string, TopicMetadata> replacement;
  for (auto& topic : topics) {
    if (!IsValidTopicName(topic.name) || topic.partition_count == 0 ||
        topic.partition_count > kMaxPartitions || !replacement.emplace(topic.name, topic).second) {
      if (error != nullptr) *error = "invalid topic metadata";
      return false;
    }
  }
  std::unique_lock lock(mutex_);
  topics_ = std::move(replacement);
  return true;
}

bool QueueManager::GetTopic(const std::string& name, TopicMetadata* topic) const {
  if (topic == nullptr) return false;
  std::shared_lock lock(mutex_);
  const auto iterator = topics_.find(name);
  if (iterator == topics_.end()) return false;
  *topic = iterator->second;
  return true;
}

std::vector<TopicMetadata> QueueManager::ListTopics() const {
  std::shared_lock lock(mutex_);
  std::vector<TopicMetadata> topics;
  topics.reserve(topics_.size());
  for (const auto& item : topics_) topics.push_back(item.second);
  std::sort(
      topics.begin(), topics.end(),
      [](const TopicMetadata& left, const TopicMetadata& right) { return left.name < right.name; });
  return topics;
}

bool QueueManager::ResolvePartition(const std::string& topic, std::uint32_t requested_partition,
                                    const std::string& key, std::uint32_t* partition,
                                    std::string* error) const {
  if (partition == nullptr) return false;
  std::shared_lock lock(mutex_);
  const auto iterator = topics_.find(topic);
  if (iterator == topics_.end()) {
    if (error != nullptr) *error = "unknown topic";
    return false;
  }
  if (requested_partition == kAutoPartition) {
    *partition = static_cast<std::uint32_t>(HashKey(key) % iterator->second.partition_count);
    return true;
  }
  if (requested_partition >= iterator->second.partition_count) {
    if (error != nullptr) *error = "partition is out of range";
    return false;
  }
  *partition = requested_partition;
  return true;
}

}  // namespace mq::core
