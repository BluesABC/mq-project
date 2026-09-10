#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mq::core {

/**
 * @brief 消费者分区分配结果
 *
 * 表示一个消费者被分配的分区信息。
 * 在消费者组的 Rebalance 过程中生成。
 */
struct GroupAssignment {
  std::string topic;            ///< Topic 名称
  std::uint32_t partition = 0;  ///< 分区号
};

/**
 * @brief 消费者组协调器
 *
 * 负责消费者组的成员管理和分区分配。
 * 实现了类似 Kafka Consumer Group 的协议。
 *
 * 核心功能：
 * 1. 成员管理：Join/Leave/Heartbeat 维护成员存活状态
 * 2. 分区分配：Rebalance 根据订阅关系均匀分配分区
 * 3. 超时检测：Expire 清理心跳超时的成员
 *
 * 在整体架构中的角色：
 * - 接收 Consumer 的 Join/Heartbeat/Leave 请求
 * - 执行 Rebalance 算法，生成分区分配方案
 * - 将分配结果返回给 Consumer
 *
 * 与其他模块的关系：
 * - Broker 调用本类处理消费者组相关请求
 * - Consumer 通过 Broker 间接调用本类
 * - 本类依赖 QueueManager 获取 Topic 的分区信息
 *
 * 线程安全性：内部使用互斥锁保护共享状态
 *
 * 设计选择：
 * - 使用简单轮询（Round-Robin）分配，保证均匀性
 * - 心跳超时机制自动移除故障消费者
 * - 支持动态订阅和取消订阅
 *
 * 【建议】当前实现是单机版本，分布式版本需要考虑：
 * - 成员状态持久化
 * - 选举协调者
 * - 处理网络分区
 */
class ConsumerGroupCoordinator {
 public:
  /**
   * @brief 时钟类型
   * 使用 steady_clock 保证时间单调递增，不受系统时间调整影响
   */
  using Clock = std::chrono::steady_clock;

  /**
   * @brief 构造函数
   *
   * @param member_timeout 成员心跳超时时间，默认 30 秒
   *        超时后成员会被认为已离开，触发 Rebalance
   */
  explicit ConsumerGroupCoordinator(std::chrono::seconds member_timeout = std::chrono::seconds(30));

  /**
   * @brief 消费者加入组
   *
   * 加入流程：
   * 1. 将成员加入组的成员列表
   * 2. 记录成员的订阅主题
   * 3. 更新最后心跳时间
   * 4. 触发 Rebalance 重新分配分区
   *
   * @param group 消费者组名称
   * @param member_id 消费者成员 ID（唯一标识）
   * @param topics 消费者订阅的 Topic 列表
   * @param now 当前时间戳，用于心跳超时检测
   * @return true 加入成功；false 参数无效或组不存在
   */
  bool Join(const std::string& group, const std::string& member_id,
            const std::vector<std::string>& topics, Clock::time_point now = Clock::now());

  /**
   * @brief 消费者心跳
   *
   * 定期调用以保持成员存活状态。
   * 如果成员已超时被移除，返回 false 表示需要重新加入。
   *
   * @param group 消费者组名称
   * @param member_id 消费者成员 ID
   * @param now 当前时间戳
   * @return true 心跳成功；false 成员不存在或已超时
   */
  bool Heartbeat(const std::string& group, const std::string& member_id,
                 Clock::time_point now = Clock::now());

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
  bool Leave(const std::string& group, const std::string& member_id);

  /**
   * @brief 清理心跳超时的成员
   *
   * 通常由 Broker 定期调用，检查并移除超时成员。
   * 移除超时成员后会触发 Rebalance。
   *
   * @param now 当前时间戳
   * @return 被移除的成员 ID 列表
   */
  std::vector<std::string> Expire(Clock::time_point now = Clock::now());

  /**
   * @brief 获取成员的分区分配结果
   *
   * 在 Join 后调用，获取 Rebalance 后的分区分配。
   *
   * @param group 消费者组名称
   * @param member_id 消费者成员 ID
   * @param assignments 输出参数，分配的分区列表
   * @return true 获取成功；false 成员不存在
   */
  bool Sync(const std::string& group, const std::string& member_id,
            std::vector<GroupAssignment>* assignments) const;

  /**
   * @brief 更新成员的订阅主题
   *
   * 允许成员在运行时动态调整订阅的 Topic。
   * 更新后会触发 Rebalance。
   *
   * @param group 消费者组名称
   * @param member_id 消费者成员 ID
   * @param topics 新的订阅主题列表
   * @return true 更新成功；false 成员不存在
   */
  bool SetSubscriptions(const std::string& group, const std::string& member_id,
                        const std::vector<std::string>& topics);

  /**
   * @brief 设置 Topic 的分区数量
   *
   * 在 Rebalance 前调用，确保分区分配基于最新的分区信息。
   *
   * @param topic Topic 名称
   * @param partition_count 分区数量
   */
  void SetPartitions(const std::string& topic, std::uint32_t partition_count);

  /**
   * @brief 获取成员的完整分配信息
   *
   * 与 Sync 类似，但返回结构化的分配信息。
   *
   * @param group 消费者组名称
   * @param member_id 消费者成员 ID
   * @return 分配结果列表，成员不存在时返回空列表
   */
  std::vector<GroupAssignment> Assignment(const std::string& group,
                                          const std::string& member_id) const;

 private:
  /**
   * @brief 成员信息结构体
   */
  struct Member {
    Clock::time_point last_heartbeat;  ///< 最后心跳时间
    std::vector<std::string> topics;   ///< 订阅的 Topic 列表
  };

  /**
   * @brief 执行 Rebalance 算法
   *
   * Rebalance 策略：
   * 1. 收集组内所有成员的订阅 Topic
   * 2. 对每个 Topic，将其所有分区均匀分配给订阅它的成员
   * 3. 使用轮询（Round-Robin）保证均匀性
   *
   * @param group 消费者组名称
   */
  void RebalanceLocked(const std::string& group);

  const std::chrono::seconds member_timeout_;  ///< 成员心跳超时时间

  /**
   * @brief 互斥锁，保护所有共享状态
   */
  mutable std::mutex mutex_;

  /**
   * @brief 成员列表
   * 结构：group -> (member_id -> Member)
   */
  std::unordered_map<std::string, std::unordered_map<std::string, Member>> members_;

  /**
   * @brief 分区分配结果缓存
   * 结构：group -> (member_id -> [GroupAssignment])
   */
  std::unordered_map<std::string, std::vector<GroupAssignment>> assignments_;

  /**
   * @brief Topic 分区数量映射
   * 用于 Rebalance 时确定分区范围
   */
  std::unordered_map<std::string, std::uint32_t> partitions_;
};

}  // namespace mq::core