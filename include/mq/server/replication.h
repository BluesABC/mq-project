#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mq::server {

/**
 * @brief 副本角色枚举
 *
 * 复制协调器只管理角色、任期、位点和投票状态，
 * 实际网络传输由 ReplicationClient 完成。
 *
 * 角色说明：
 * - kLeader：领导者，接收写请求并复制到 Follower
 * - kFollower：跟随者，接收 Leader 的日志并应用
 * - kCandidate：候选者，正在参与选举
 */
enum class ReplicaRole { kLeader, kFollower, kCandidate };

/**
 * @brief 副本同步进度
 *
 * 记录每个副本节点的同步状态。
 */
struct ReplicaProgress {
  std::string node_id;       ///< 节点 ID
  std::uint64_t replicated_offset = 0;  ///< 已复制的 offset
  std::chrono::steady_clock::time_point last_heartbeat;  ///< 最后心跳时间
  bool healthy = false;      ///< 是否健康（心跳是否超时）
  std::uint64_t term = 0;    ///< 当前任期
};

/**
 * @brief 选举结果
 *
 * 记录选举的任期和候选人信息。
 */
struct ElectionResult {
  std::uint64_t term = 0;           ///< 选举任期
  std::string candidate_id;         ///< 候选人 ID
};

/**
 * @brief 复制协调器
 *
 * 实现类 Raft 的复制协议，管理 Leader 选举和日志复制。
 *
 * 核心功能：
 * 1. Leader 选举：通过投票机制选出 Leader
 * 2. 日志复制：Leader 将日志复制到 Follower
 * 3. 状态机同步：提交的日志应用到本地状态机
 * 4. 故障检测：通过心跳检测节点故障
 *
 * 在整体架构中的角色：
 * - Broker 调用本类进行复制协调
 * - ReplicationClient 负责实际的网络传输
 * - 本类只维护状态，不直接进行网络通信
 *
 * 与其他模块的关系：
 * - Broker 的 ReplicationLoop 调用本类
 * - ReplicationClient 根据本类的状态执行复制
 * - StorageEngine 存储复制的日志
 *
 * Raft 协议要点：
 * - Leader 接收所有写请求
 * - Leader 将日志复制到 majority 的 Follower
 * - 只有 majority 确认后才提交日志
 * - Follower 超时未收到心跳会触发选举
 *
 * 与标准 Raft 的差异：
 * - 支持 PreVote 机制，减少不必要的选举
 * - 支持分区级别的复制跟踪
 * - 心跳超时使用随机化，避免选举冲突
 *
 * 线程安全性：
 * - 内部使用互斥锁保护所有状态
 * - 可从多个线程并发调用
 *
 * 持久化：
 * - 选举状态（term、votedFor）持久化到磁盘
 * - 重启后恢复状态，保证一致性
 */
class ReplicationCoordinator {
 public:
  /**
   * @brief 分区键类型
   * 使用字符串表示，格式为 "topic:partition"
   */
  using PartitionKey = std::string;

  /**
   * @brief 构造函数
   *
   * @param node_id 当前节点 ID
   * @param role 初始角色，默认为 Leader
   * @param heartbeat_timeout 心跳超时时间，默认 10 秒
   * @param metadata_dir 元数据目录，用于持久化选举状态
   */
  ReplicationCoordinator(std::string node_id, ReplicaRole role = ReplicaRole::kLeader,
                         std::chrono::milliseconds heartbeat_timeout = std::chrono::seconds(10),
                         std::filesystem::path metadata_dir = {});

  /**
   * @brief 获取当前角色
   * @return 当前的副本角色
   */
  ReplicaRole role() const;

  /**
   * @brief 获取当前节点 ID
   * @return 节点 ID
   */
  std::string nodeId() const;

  /**
   * @brief 获取 Leader ID
   * @return Leader 节点 ID，无 Leader 时返回空字符串
   */
  std::string leaderId() const;

  /**
   * @brief 获取当前任期
   * @return 任期编号
   */
  std::uint64_t term() const;

  /**
   * @brief 获取最后一条日志的索引
   * @return 日志索引
   */
  std::uint64_t lastLogIndex() const;

  /**
   * @brief 获取最后一条日志的任期
   * @return 日志任期
   */
  std::uint64_t lastLogTerm() const;

  /**
   * @brief 获取全局提交索引
   * @return 已提交的最高日志索引
   */
  std::uint64_t commitIndex() const;

  /**
   * @brief 获取指定分区的提交索引
   *
   * @param partition 分区键
   * @return 已提交的最高日志索引
   */
  std::uint64_t commitIndex(const PartitionKey& partition) const;

  /**
   * @brief 获取最后应用的日志索引
   * @return 已应用到状态机的最高日志索引
   */
  std::uint64_t lastApplied() const;

  /**
   * @brief 获取投票给的节点 ID
   * @return 当前任期投票的节点 ID
   */
  std::string votedFor() const;

  /**
   * @brief 检查是否可以接受写请求
   *
   * 只有 Leader 可以接受写请求。
   *
   * @return true 可以写入；false 不能写入
   */
  bool CanServeWrites() const;

  /**
   * @brief 设置 Leader
   *
   * @param node_id Leader 节点 ID
   */
  void SetLeader(std::string node_id);

  /**
   * @brief 注册副本节点
   *
   * @param node_id 副本节点 ID
   */
  void RegisterReplica(std::string node_id);

  /**
   * @brief 移除副本节点
   *
   * @param node_id 副本节点 ID
   */
  void RemoveReplica(const std::string& node_id);

  /**
   * @brief 观察心跳（全局）
   *
   * 更新副本的同步状态。
   *
   * @param node_id 副本节点 ID
   * @param replicated_offset 已复制的 offset
   * @param now 当前时间
   */
  void ObserveHeartbeat(
      const std::string& node_id, std::uint64_t replicated_offset,
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

  /**
   * @brief 观察心跳（分区级别）
   *
   * @param partition 分区键
   * @param node_id 副本节点 ID
   * @param replicated_offset 已复制的 offset
   * @param now 当前时间
   */
  void ObserveHeartbeat(
      const PartitionKey& partition, const std::string& node_id, std::uint64_t replicated_offset,
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

  /**
   * @brief 观察日志追加（全局）
   *
   * Leader 调用此方法记录日志追加状态。
   *
   * @param term 任期
   * @param leader_id Leader ID
   * @param replicated_offset 已复制的 offset
   * @param leader_commit Leader 的提交索引
   * @param now 当前时间
   * @return true 追加成功；false 任期不匹配
   */
  bool ObserveAppend(std::uint64_t term, const std::string& leader_id,
                     std::uint64_t replicated_offset, std::uint64_t leader_commit,
                     std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

  /**
   * @brief 观察日志追加（分区级别）
   *
   * @param partition 分区键
   * @param term 任期
   * @param leader_id Leader ID
   * @param replicated_offset 已复制的 offset
   * @param leader_commit Leader 的提交索引
   * @param now 当前时间
   * @return true 追加成功；false 任期不匹配
   */
  bool ObserveAppend(const PartitionKey& partition, std::uint64_t term,
                     const std::string& leader_id, std::uint64_t replicated_offset,
                     std::uint64_t leader_commit,
                     std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

  /**
   * @brief 开始选举
   *
   * 候选者调用此方法发起选举。
   *
   * @return 选举结果，包含新的任期
   */
  ElectionResult BeginElection();

  /**
   * @brief 开始预投票
   *
   * PreVote 机制：在正式选举前先收集预投票，
   * 如果预投票失败则不增加任期，减少不必要的选举。
   *
   * @return 预投票结果
   */
  ElectionResult BeginPreVote() const;

  /**
   * @brief 观察预投票结果
   *
   * @param term 预投票任期
   * @param voter_id 投票者 ID
   * @param granted 是否授予投票
   * @return true 获得多数预投票；false 未获得多数
   */
  bool ObservePreVote(std::uint64_t term, const std::string& voter_id, bool granted);

  /**
   * @brief 请求投票
   *
   * 标准 Raft 投票请求。
   *
   * @param term 请求者的任期
   * @param candidate_id 候选者 ID
   * @return true 授予投票；false 拒绝投票
   */
  bool RequestVote(std::uint64_t term, const std::string& candidate_id);

  /**
   * @brief 请求投票（带日志信息）
   *
   * 根据候选者的日志新旧程度决定是否投票。
   *
   * @param term 请求者的任期
   * @param candidate_id 候选者 ID
   * @param candidate_last_log_index 候选者最后日志索引
   * @param candidate_last_log_term 候选者最后日志任期
   * @return true 授予投票；false 拒绝投票
   */
  bool RequestVote(std::uint64_t term, const std::string& candidate_id,
                   std::uint64_t candidate_last_log_index, std::uint64_t candidate_last_log_term);

  /**
   * @brief 请求预投票
   *
   * @param term 预投票任期
   * @param candidate_id 候选者 ID
   * @param candidate_last_log_index 候选者最后日志索引
   * @param candidate_last_log_term 候选者最后日志任期
   * @return true 授予预投票；false 拒绝预投票
   */
  bool RequestPreVote(std::uint64_t term, const std::string& candidate_id,
                      std::uint64_t candidate_last_log_index,
                      std::uint64_t candidate_last_log_term) const;

  /**
   * @brief 观察投票结果
   *
   * @param term 任期
   * @param voter_id 投票者 ID
   * @param granted 是否授予投票
   * @return true 获得多数投票；false 未获得多数
   */
  bool ObserveVote(std::uint64_t term, const std::string& voter_id, bool granted);

  /**
   * @brief 检查日志是否匹配
   *
   * 用于日志复制时的一致性检查。
   *
   * @param partition 分区键
   * @param prev_log_index 前一条日志的索引
   * @param prev_log_term 前一条日志的任期
   * @return true 日志匹配；false 不匹配
   */
  bool LogMatches(const PartitionKey& partition, std::uint64_t prev_log_index,
                  std::uint64_t prev_log_term) const;

  /**
   * @brief 获取下一个日志索引
   *
   * @param partition 分区键
   * @return 下一个要追加的日志索引
   */
  std::uint64_t NextLogIndex(const PartitionKey& partition) const;

  /**
   * @brief 观察心跳轮次
   *
   * Leader 调用，记录心跳响应情况。
   *
   * @param majority_responded 是否收到多数派响应
   * @return true 多数派响应；false 未收到多数派响应
   */
  bool ObserveHeartbeatRound(bool majority_responded);

  /**
   * @brief 推进提交索引（全局）
   *
   * @param offset 新的提交索引
   * @return true 推进成功；false 偏移量无效
   */
  bool AdvanceCommit(std::uint64_t offset);

  /**
   * @brief 推进提交索引（分区级别）
   *
   * @param partition 分区键
   * @param offset 新的提交索引
   * @return true 推进成功；false 偏移量无效
   */
  bool AdvanceCommit(const PartitionKey& partition, std::uint64_t offset);

  /**
   * @brief 记录本地偏移量（全局）
   *
   * @param offset 本地写入的 offset
   */
  void RecordLocalOffset(std::uint64_t offset);

  /**
   * @brief 记录本地偏移量（分区级别）
   *
   * @param partition 分区键
   * @param offset 本地写入的 offset
   */
  void RecordLocalOffset(const PartitionKey& partition, std::uint64_t offset);

  /**
   * @brief 检查是否达到 Quorum 复制
   *
   * @param offset 要检查的 offset
   * @param replica_count 副本总数
   * @return true 达到 Quorum；false 未达到
   */
  bool IsQuorumReplicated(std::uint64_t offset, std::size_t replica_count) const;

  /**
   * @brief 检查是否达到 Quorum 复制（分区级别）
   *
   * @param partition 分区键
   * @param offset 要检查的 offset
   * @param replica_count 副本总数
   * @return true 达到 Quorum；false 未达到
   */
  bool IsQuorumReplicated(const PartitionKey& partition, std::uint64_t offset,
                          std::size_t replica_count) const;

  /**
   * @brief 选举 Leader
   *
   * 根据健康状态和日志新旧程度选举 Leader。
   *
   * @param now 当前时间
   * @return 选出的 Leader ID，无合适 Leader 时返回空字符串
   */
  std::string ElectLeader(
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

  /**
   * @brief 获取所有副本的快照
   *
   * @param now 当前时间
   * @return 副本进度列表
   */
  std::vector<ReplicaProgress> Snapshot(
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) const;

 private:
  /**
   * @brief 检查副本是否健康
   *
   * @param replica 副本进度
   * @param now 当前时间
   * @return true 健康；false 不健康
   */
  bool Healthy(const ReplicaProgress& replica, std::chrono::steady_clock::time_point now) const;

  /**
   * @brief 计算多数派阈值
   * @return 多数派所需的最小节点数
   */
  std::size_t Majority() const;

  /**
   * @brief 持久化状态到磁盘
   */
  void PersistLocked() const;

  /**
   * @brief 从磁盘加载状态
   */
  void LoadState();

  // ==================== 成员变量 ====================

  const std::string node_id_;                       ///< 当前节点 ID
  const std::chrono::milliseconds heartbeat_timeout_;  ///< 心跳超时时间

  mutable std::mutex mutex_;                        ///< 互斥锁

  ReplicaRole role_;                                ///< 当前角色
  std::string leader_id_;                           ///< Leader ID
  std::string voted_for_;                           ///< 当前任期投票的节点

  std::uint64_t current_term_ = 0;                  ///< 当前任期
  std::uint64_t local_offset_ = 0;                  ///< 本地写入的 offset
  std::uint64_t commit_index_ = 0;                  ///< 全局提交索引
  std::uint64_t last_applied_ = 0;                  ///< 最后应用的索引

  std::unordered_set<std::string> votes_;           ///< 选举投票
  std::unordered_set<std::string> pre_votes_;       ///< 预投票

  std::size_t missed_heartbeat_rounds_ = 0;         ///< 连续未响应的心跳轮次

  const std::filesystem::path state_path_;          ///< 状态持久化路径

  std::unordered_map<std::string, ReplicaProgress> replicas_;  ///< 副本进度

  /**
   * @brief 分区级别状态
   */
  struct PartitionState {
    std::uint64_t local_offset = 0;       ///< 本地 offset
    std::uint64_t commit_index = 0;       ///< 提交索引
    std::uint64_t last_applied = 0;       ///< 最后应用的索引
    std::uint64_t last_log_term = 0;      ///< 最后日志的任期
  };

  std::unordered_map<PartitionKey, PartitionState> partition_states_;  ///< 分区状态
  std::unordered_map<PartitionKey, std::unordered_map<std::string, ReplicaProgress>>
      partition_replicas_;  ///< 分区级别的副本进度
};
}  // namespace mq::server