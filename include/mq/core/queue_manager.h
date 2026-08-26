#pragma once

#include <cstdint>
#include <limits>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mq::core {

// QueueManager 只维护 Topic 元数据和分区路由，不直接持有消息数据。
struct TopicMetadata {
  std::string name;
  std::uint32_t partition_count = 0;
};

class QueueManager {
 public:
  static constexpr std::uint32_t kAutoPartition = std::numeric_limits<std::uint32_t>::max();

  bool CreateTopic(std::string name, std::uint32_t partition_count, std::string* error = nullptr);
  bool DeleteTopic(const std::string& name, std::string* error = nullptr);
  bool ReplaceTopics(std::vector<TopicMetadata> topics, std::string* error = nullptr);
  bool GetTopic(const std::string& name, TopicMetadata* topic) const;
  std::vector<TopicMetadata> ListTopics() const;
  // 指定分区优先；自动分区时使用 key 保持同一业务键落在同一分区。
  bool ResolvePartition(const std::string& topic, std::uint32_t requested_partition,
                        const std::string& key, std::uint32_t* partition,
                        std::string* error = nullptr) const;

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, TopicMetadata> topics_;
};

}  // namespace mq::core
