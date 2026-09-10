#pragma once

#include <cstdint>
#include <limits>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mq::core {

/**
 * @brief Topic 元数据结构
 *
 * 存储 Topic 的基本信息，包括名称和分区数量。
 * QueueManager 只维护这些元数据用于路由决策，不直接持有消息数据。
 * 消息数据的存储由 StorageEngine 负责，实现关注点分离。
 */
struct TopicMetadata {
  std::string name;                   ///< Topic 名称，作为唯一标识
  std::uint32_t partition_count = 0;  ///< 该 Topic 的分区数量，必须大于 0
};

/**
 * @brief 队列管理器
 *
 * 负责 Topic 的元数据管理和分区路由决策。
 * 在整体架构中的角色：
 * - 接收 Producer/Consumer 的分区解析请求
 * - 根据 Topic 配置和消息 Key 计算目标分区
 * - 维护 Topic 的创建/删除/查询等生命周期操作
 *
 * 与其他模块的关系：
 * - Server 层在处理 Produce/Consume 请求时调用本类解析分区
 * - StorageEngine 根据分区信息写入对应分区的存储文件
 * - 本类不依赖任何网络或存储实现，仅维护内存中的元数据视图
 *
 * 线程安全性：使用 std::shared_mutex 实现读写锁，
 * 读操作（Get/List/Resolve）可并发执行，写操作（Create/Delete/Replace）互斥。
 */
class QueueManager {
 public:
  /**
   * @brief 自动分区标识常量
   *
   * 当 Producer 不指定分区时，使用此值表示自动分区模式。
   * 此时会根据消息 Key 的哈希值自动选择分区，保证相同 Key 的消息落在同一分区。
   * 使用 uint32_t 最大值作为哨兵值，因为实际分区数不可能达到此值。
   */
  static constexpr std::uint32_t kAutoPartition = std::numeric_limits<std::uint32_t>::max();

  /**
   * @brief 创建 Topic
   *
   * @param name Topic 名称，必须通过 IsValidTopicName 校验
   * @param partition_count 分区数量，取值范围 [1, 1024]
   * @param error 可选的错误信息输出参数，失败时写入错误描述
   * @return true 创建成功；false 创建失败（参数无效或 Topic 已存在）
   *
   * 失败场景：
   * - name 不符合命名规范
   * - partition_count 为 0 或超过上限
   * - Topic 已存在（name 冲突）
   */
  bool CreateTopic(std::string name, std::uint32_t partition_count, std::string* error = nullptr);

  /**
   * @brief 删除 Topic
   *
   * @param name 要删除的 Topic 名称
   * @param error 可选的错误信息输出参数
   * @return true 删除成功；false Topic 不存在
   */
  bool DeleteTopic(const std::string& name, std::string* error = nullptr);

  /**
   * @brief 批量替换所有 Topic（全量覆盖）
   *
   * 用于配置热加载场景，原子性地替换整个 Topic 列表。
   * 替换前会先校验所有新 Topic 的合法性，任一无效则整体失败。
   *
   * @param topics 新的 Topic 列表，将完全覆盖现有配置
   * @param error 可选的错误信息输出参数
   * @return true 替换成功；false 校验失败或参数无效
   */
  bool ReplaceTopics(std::vector<TopicMetadata> topics, std::string* error = nullptr);

  /**
   * @brief 查询单个 Topic 的元数据
   *
   * @param name Topic 名称
   * @param topic 输出参数，成功时写入 Topic 元数据
   * @return true 找到并返回；false Topic 不存在或 topic 参数为空
   */
  bool GetTopic(const std::string& name, TopicMetadata* topic) const;

  /**
   * @brief 列出所有 Topic，按名称字母序排序
   *
   * @return Topic 元数据列表，已按 name 升序排序
   */
  std::vector<TopicMetadata> ListTopics() const;

  /**
   * @brief 解析消息应该写入/读取的分区号
   *
   * 这是分区路由的核心方法，支持两种模式：
   * 1. 指定分区：requested_partition 为具体分区号时直接使用
   * 2. 自动分区：requested_partition == kAutoPartition 时，根据 key 的哈希值计算
   *
   * @param topic Topic 名称
   * @param requested_partition 请求的分区号，kAutoPartition 表示自动分区
   * @param key 消息键，自动分区时用于哈希计算；指定分区时可为空
   * @param partition 输出参数，成功时写入最终确定的分区号
   * @param error 可选的错误信息输出参数
   * @return true 解析成功；false Topic 不存在或分区号越界
   */
  bool ResolvePartition(const std::string& topic, std::uint32_t requested_partition,
                        const std::string& key, std::uint32_t* partition,
                        std::string* error = nullptr) const;

 private:
  /**
   * 读写锁，保护 topics_ 的并发访问
   * - 读操作使用 shared_lock（允许多个读线程并发）
   * - 写操作使用 unique_lock（独占访问）
   *
   * 设计选择：使用 shared_mutex 而非 mutex，因为 Topic 元数据的读写比很高
   * （大量 Produce/Consume 请求读取分区信息，但 Topic 创建/删除很少发生）。
   */
  mutable std::shared_mutex mutex_;

  /**
   * Topic 名称到元数据的映射表
   * 使用 unordered_map 实现 O(1) 平均查找复杂度
   */
  std::unordered_map<std::string, TopicMetadata> topics_;
};

}  // namespace mq::core