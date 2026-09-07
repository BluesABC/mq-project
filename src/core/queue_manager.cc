#include "mq/core/queue_manager.h"

#include <algorithm>
#include <mutex>
#include <utility>

#include "mq/core/topic.h"

namespace mq::core {
namespace {

/**
 * @brief 最大分区数量限制
 *
 * 防止单个 Topic 创建过多分区导致系统资源耗尽。
 * 1024 是一个合理的上限，足以满足大多数高并发场景，
 * 同时避免元数据占用过多内存。
 */
constexpr std::uint32_t kMaxPartitions = 1024;

/**
 * @brief FNV-1a 哈希函数实现
 *
 * 用于自动分区时计算消息 Key 的哈希值。
 * 选择 FNV-1a 的原因：
 * 1. 计算速度快，适合热路径调用
 * 2. 分布均匀，减少分区热点问题
 * 3. 实现简单，无需依赖第三方库
 *
 * @param key 待哈希的字符串键
 * @return 64 位哈希值
 *
 * 【建议】如果对哈希分布有更高要求，可考虑使用 xxHash 或 MurmurHash，
 * 但 FNV-1a 在当前场景下已经足够。
 */
std::uint64_t HashKey(const std::string& key) {
  // FNV 哈希的初始值和质数常量
  std::uint64_t hash = 14695981039346656037ull;  // FNV offset basis
  for (const unsigned char byte : key) {
    hash ^= byte;                                // 异或当前字节
    hash *= 1099511628211ull;                    // 乘以 FNV prime
  }
  return hash;
}

}  // namespace

bool QueueManager::CreateTopic(std::string name, std::uint32_t partition_count,
                               std::string* error) {
  // 参数校验：Topic 名称必须合法，分区数必须在有效范围内
  if (!IsValidTopicName(name) || partition_count == 0 || partition_count > kMaxPartitions) {
    if (error != nullptr) *error = "invalid topic metadata";
    return false;
  }

  // 获取写锁，独占访问 topics_ 映射表
  std::unique_lock lock(mutex_);

  // 构造元数据并尝试插入，emplace 返回 insertion iterator 和 success flag
  TopicMetadata metadata{name, partition_count};
  const auto result = topics_.emplace(std::move(name), std::move(metadata));

  // 如果插入失败（Topic 已存在），记录错误
  if (!result.second && error != nullptr) *error = "topic already exists";
  return result.second;
}

bool QueueManager::DeleteTopic(const std::string& name, std::string* error) {
  // 获取写锁，因为要修改 topics_
  std::unique_lock lock(mutex_);

  // erase 返回删除的元素数量，0 表示元素不存在
  if (topics_.erase(name) == 0) {
    if (error != nullptr) *error = "unknown topic";
    return false;
  }
  return true;
}

bool QueueManager::ReplaceTopics(std::vector<TopicMetadata> topics, std::string* error) {
  // 先在临时映射表中校验所有 Topic，避免部分修改导致数据不一致
  std::unordered_map<std::string, TopicMetadata> replacement;

  for (auto& topic : topics) {
    // 校验每个 Topic 的合法性：名称有效、分区数在范围内、无重复名称
    if (!IsValidTopicName(topic.name) || topic.partition_count == 0 ||
        topic.partition_count > kMaxPartitions || !replacement.emplace(topic.name, topic).second) {
      if (error != nullptr) *error = "invalid topic metadata";
      return false;
    }
  }

  // 获取写锁，原子性地替换整个映射表
  std::unique_lock lock(mutex_);
  topics_ = std::move(replacement);
  return true;
}

bool QueueManager::GetTopic(const std::string& name, TopicMetadata* topic) const {
  // 空指针检查
  if (topic == nullptr) return false;

  // 获取读锁，允许多个读线程并发访问
  std::shared_lock lock(mutex_);

  // 在映射表中查找 Topic
  const auto iterator = topics_.find(name);
  if (iterator == topics_.end()) return false;

  // 复制元数据到输出参数（值拷贝，避免暴露内部数据）
  *topic = iterator->second;
  return true;
}

std::vector<TopicMetadata> QueueManager::ListTopics() const {
  // 获取读锁
  std::shared_lock lock(mutex_);

  // 预分配内存，避免多次扩容
  std::vector<TopicMetadata> topics;
  topics.reserve(topics_.size());

  // 复制所有 Topic 元数据
  for (const auto& item : topics_.topics_) topics.push_back(item.second);

  // 按名称排序，保证返回结果的确定性，便于客户端展示和比较
  std::sort(
      topics.begin(), topics.end(),
      [](const TopicMetadata& left, const TopicMetadata& right) { return left.name < right.name; });
  return topics;
}

bool QueueManager::ResolvePartition(const std::string& topic, std::uint32_t requested_partition,
                                    const std::string& key, std::uint32_t* partition,
                                    std::string* error) const {
  // 空指针检查
  if (partition == nullptr) return false;

  // 获取读锁，分区解析是高频操作，读锁允许多个 Producer/Consumer 并发调用
  std::shared_lock lock(mutex_);

  // 查找 Topic 元数据
  const auto iterator = topics_.find(topic);
  if (iterator == topics_.end()) {
    if (error != nullptr) *error = "unknown topic";
    return false;
  }

  // 自动分区模式：根据消息 Key 的哈希值计算目标分区
  // 使用取模运算确保结果在 [0, partition_count) 范围内
  if (requested_partition == kAutoPartition) {
    *partition = static_cast<std::uint32_t>(HashKey(key) % iterator->second.partition_count);
    return true;
  }

  // 指定分区模式：校验分区号是否越界
  if (requested_partition >= iterator->second.partition_count) {
    if (error != nullptr) *error = "partition is out of range";
    return false;
  }

  // 直接使用指定的分区号
  *partition = requested_partition;
  return true;
}

}  // namespace mq::core