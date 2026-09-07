#include "mq/core/consumer_group_coordinator.h"

#include <algorithm>
#include <set>

namespace mq::core {

ConsumerGroupCoordinator::ConsumerGroupCoordinator(std::chrono::seconds member_timeout)
    : member_timeout_(member_timeout) {}

/**
 * @brief 消费者加入组
 *
 * 加入流程：
 * 1. 校验参数
 * 2. 将成员加入组的成员列表
 * 3. 记录成员的订阅主题和最后心跳时间
 * 4. 触发 Rebalance 重新分配分区
 *
 * @param group 消费者组名称
 * @param member_id 消费者成员 ID
 * @param topics 消费者订阅的 Topic 列表
 * @param now 当前时间戳
 * @return true 加入成功；false 参数无效
 */
bool ConsumerGroupCoordinator::Join(const std::string& group, const std::string& member_id,
                                     const std::vector<std::string>& topics, Clock::time_point now) {
  // 参数校验
  if (group.empty() || member_id.empty()) return false;

  std::lock_guard lock(mutex_);

  // 添加成员到组
  members_[group][member_id] = {now, topics};

  // 触发 Rebalance
  RebalanceLocked(group);

  return true;
}

/**
 * @brief 消费者心跳
 *
 * 定期调用以保持成员存活状态。
 *
 * @param group 消费者组名称
 * @param member_id 消费者成员 ID
 * @param now 当前时间戳
 * @return true 心跳成功；false 成员不存在
 */
bool ConsumerGroupCoordinator::Heartbeat(const std::string& group, const std::string& member_id,
                                         Clock::time_point now) {
  std::lock_guard lock(mutex_);

  // 查找组
  const auto group_it = members_.find(group);
  if (group_it == members_.end()) return false;

  // 查找成员
  const auto member_it = group_it->second.find(member_id);
  if (member_it == group_it->second.end()) return false;

  // 更新最后心跳时间
  member_it->second.last_heartbeat = now;

  return true;
}

/**
 * @brief 消费者主动离开组
 *
 * 离开流程：
 * 1. 从成员列表中移除该成员
 * 2. 触发 Rebalance 重新分配分区
 *
 * @param group 消费者组名称
 * @param member_id 消费者成员 ID
 * @return true 离开成功；false 成员不存在
 */
bool ConsumerGroupCoordinator::Leave(const std::string& group, const std::string& member_id) {
  std::lock_guard lock(mutex_);

  // 查找组
  const auto group_it = members_.find(group);
  if (group_it == members_.end()) return false;

  // 移除成员
  if (group_it->second.erase(member_id) == 0) return false;

  // 触发 Rebalance
  RebalanceLocked(group);

  return true;
}

/**
 * @brief 清理心跳超时的成员
 *
 * 遍历所有组和成员，移除心跳超时的成员。
 * 移除后触发 Rebalance。
 *
 * @param now 当前时间戳
 * @return 被移除的成员 ID 列表（格式：group\x1fmember_id）
 */
std::vector<std::string> ConsumerGroupCoordinator::Expire(Clock::time_point now) {
  std::lock_guard lock(mutex_);
  std::vector<std::string> expired;

  // 遍历所有组
  for (auto& [group, members] : members_) {
    // 遍历组内成员
    for (auto it = members.begin(); it != members.end();) {
      // 检查心跳是否超时
      if (now - it->second.last_heartbeat > member_timeout_) {
        expired.push_back(group + "\x1f" + it->first);
        it = members.erase(it);
      } else {
        ++it;
      }
    }

    // 触发 Rebalance
    RebalanceLocked(group);
  }

  return expired;
}

/**
 * @brief 设置 Topic 的分区数量
 *
 * 在 Rebalance 前调用，确保分区分配基于最新的分区信息。
 *
 * @param topic Topic 名称
 * @param partition_count 分区数量
 */
void ConsumerGroupCoordinator::SetPartitions(const std::string& topic,
                                              std::uint32_t partition_count) {
  std::lock_guard lock(mutex_);

  // 更新分区数量
  partitions_[topic] = partition_count;

  // 对所有订阅该 Topic 的组触发 Rebalance
  for (const auto& [group, members] : members_) RebalanceLocked(group);
}

/**
 * @brief 更新成员的订阅主题
 *
 * @param group 消费者组名称
 * @param member_id 消费者成员 ID
 * @param topics 新的订阅主题列表
 * @return true 更新成功；false 成员不存在
 */
bool ConsumerGroupCoordinator::SetSubscriptions(const std::string& group,
                                                const std::string& member_id,
                                                const std::vector<std::string>& topics) {
  std::lock_guard lock(mutex_);

  // 查找组
  auto group_it = members_.find(group);
  if (group_it == members_.end() || group_it->second.find(member_id) == group_it->second.end())
    return false;

  // 更新订阅主题
  group_it->second[member_id].topics = topics;

  // 触发 Rebalance
  RebalanceLocked(group);

  return true;
}

/**
 * @brief 执行 Rebalance 算法
 *
 * Rebalance 策略：
 * 1. 收集组内所有成员的订阅 Topic
 * 2. 对每个 Topic，将其所有分区收集到列表
 * 3. 按 Topic 名称和分区号排序
 * 4. 将分区列表平铺，准备轮询分配
 *
 * 注意：实际的分区-成员映射在 Sync 方法中完成，
 * 这里只是生成全局的分区列表。
 *
 * @param group 消费者组名称
 */
void ConsumerGroupCoordinator::RebalanceLocked(const std::string& group) {
  const auto group_it = members_.find(group);
  if (group_it == members_.end()) return;

  // 收集所有成员 ID（排序以保证确定性）
  std::vector<std::string> member_ids;
  for (const auto& [member_id, member] : group_it->second) member_ids.push_back(member_id);
  std::sort(member_ids.begin(), member_ids.end());

  auto& group_assignments = assignments_[group];
  group_assignments.clear();

  if (member_ids.empty()) return;

  // 收集所有订阅的 Topic
  std::vector<GroupAssignment> all_partitions;
  std::set<std::string> subscribed_topics;
  for (const auto& [member_id, member] : group_it->second)
    subscribed_topics.insert(member.topics.begin(), member.topics.end());

  // 收集所有分区
  for (const auto& topic : subscribed_topics) {
    const auto count = partitions_.count(topic) == 0 ? 0 : partitions_.at(topic);
    for (std::uint32_t partition = 0; partition < count; ++partition)
      all_partitions.push_back({topic, partition});
  }

  // 按 Topic 名称和分区号排序
  std::sort(all_partitions.begin(), all_partitions.end(), [](const auto& left, const auto& right) {
    return left.topic == right.topic ? left.partition < right.partition : left.topic < right.topic;
  });

  // 将所有分区添加到分配列表
  for (std::size_t index = 0; index < all_partitions.size(); ++index) {
    group_assignments.push_back(all_partitions[index]);
  }
}

/**
 * @brief 获取成员的分区分配结果
 *
 * 分配策略：轮询（Round-Robin）
 * 1. 将成员 ID 排序
 * 2. 找到当前成员在排序后的位置
 * 3. 从该位置开始，每隔 member_count 个分区取一个
 *
 * 示例：3 个成员，6 个分区
 * - 成员 0 获取分区：0, 3
 * - 成员 1 获取分区：1, 4
 * - 成员 2 获取分区：2, 5
 *
 * @param group 消费者组名称
 * @param member_id 消费者成员 ID
 * @param assignments 输出参数，分配的分区列表
 * @return true 获取成功；false 成员不存在
 */
bool ConsumerGroupCoordinator::Sync(const std::string& group, const std::string& member_id,
                                    std::vector<GroupAssignment>* assignments) const {
  if (assignments == nullptr) return false;

  std::lock_guard lock(mutex_);

  // 查找组
  const auto group_it = members_.find(group);
  if (group_it == members_.end() || group_it->second.find(member_id) == group_it->second.end())
    return false;

  // 查找分配列表
  const auto all_it = assignments_.find(group);
  assignments->clear();
  if (all_it == assignments_.end()) return true;

  // 排序成员 ID 以保证确定性
  std::vector<std::string> sorted;
  for (const auto& [id, member] : group_it->second) sorted.push_back(id);
  std::sort(sorted.begin(), sorted.end());

  // 找到当前成员的位置
  const auto found = std::find(sorted.begin(), sorted.end(), member_id);
  if (found == sorted.end()) return false;
  const auto member_index = static_cast<std::size_t>(found - sorted.begin());

  // 轮询分配：从 member_index 开始，每隔 sorted.size() 取一个
  for (std::size_t index = member_index; index < all_it->second.size(); index += sorted.size())
    assignments->push_back(all_it->second[index]);

  return true;
}

/**
 * @brief 获取成员的完整分配信息
 *
 * 与 Sync 类似，但返回结构化的分配信息。
 *
 * @param group 消费者组名称
 * @param member_id 消费者成员 ID
 * @return 分配结果列表，成员不存在时返回空列表
 */
std::vector<GroupAssignment> ConsumerGroupCoordinator::Assignment(
    const std::string& group, const std::string& member_id) const {
  std::lock_guard lock(mutex_);

  // 查找组
  const auto member_it = members_.find(group);
  if (member_it == members_.end() || member_it->second.find(member_id) == member_it->second.end())
    return {};

  // 查找分配列表
  const auto all_it = assignments_.find(group);
  if (all_it == assignments_.end()) return {};

  // 排序成员 ID
  std::vector<GroupAssignment> result;
  std::size_t member_index = 0;
  std::vector<std::string> sorted;
  for (const auto& [id, member] : member_it->second) sorted.push_back(id);
  std::sort(sorted.begin(), sorted.end());

  // 找到当前成员的位置
  const auto found = std::find(sorted.begin(), sorted.end(), member_id);
  if (found == sorted.end()) return {};
  member_index = static_cast<std::size_t>(found - sorted.begin());

  // 轮询分配
  for (std::size_t index = member_index; index < all_it->second.size(); index += sorted.size())
    result.push_back(all_it->second[index]);

  return result;
}

}  // namespace mq::core