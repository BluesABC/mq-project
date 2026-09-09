/**
 * @brief MQ 复制协调器实现
 *
 * ReplicationCoordinator 负责管理分布式副本复制的核心逻辑：
 * 1. 节点角色管理：Leader/Follower/Candidate 状态转换
 * 2. 任期管理：单调递增的逻辑时钟，用于选举和请求验证
 * 3. 选举机制：预投票 + 正式投票，确保日志完整性
 * 4. 提交索引：基于多数派确认推进 commit index
 * 5. 健康监控：心跳超时检测和副本状态跟踪
 * 6. 状态持久化：关键状态写入磁盘，支持崩溃恢复
 *
 * 本实现是简化版的 Raft 协议，适用于消息队列的高可用场景。
 */

#include "mq/server/replication.h"

#include <algorithm>
#include <fstream>
#include <system_error>
#include <utility>

namespace mq::server {
namespace {

/// 遗留分区键，用于向后兼容
const ReplicationCoordinator::PartitionKey kLegacyPartition = "__legacy__";

}  // namespace

/**
 * @brief 构造函数，初始化复制协调器
 *
 * @param node_id 节点 ID
 * @param role 初始角色
 * @param timeout 心跳超时时间
 * @param metadata_dir 元数据目录
 */
ReplicationCoordinator::ReplicationCoordinator(std::string node_id, ReplicaRole role,
                                               std::chrono::milliseconds timeout,
                                               std::filesystem::path metadata_dir)
    : node_id_(std::move(node_id)),
      heartbeat_timeout_(timeout),
      role_(role),
      leader_id_(node_id_),
      state_path_(metadata_dir.empty() ? std::filesystem::path{}
                                       : metadata_dir / "raft_state.bin") {
  LoadState();
}

/**
 * @brief 从磁盘加载 Raft 状态（任期、提交索引、投票记录）
 */
void ReplicationCoordinator::LoadState() {
  if (state_path_.empty()) return;
  std::ifstream input(state_path_, std::ios::binary);
  std::uint32_t magic = 0;
  std::uint64_t term = 0, commit = 0, applied = 0;
  std::uint32_t voted_size = 0;
  if (!input.read(reinterpret_cast<char*>(&magic), sizeof(magic)) || magic != 0x31544652u ||
      !input.read(reinterpret_cast<char*>(&term), sizeof(term)) ||
      !input.read(reinterpret_cast<char*>(&commit), sizeof(commit)) ||
      !input.read(reinterpret_cast<char*>(&applied), sizeof(applied)) ||
      !input.read(reinterpret_cast<char*>(&voted_size), sizeof(voted_size)) || voted_size > 4096)
    return;
  std::string voted(voted_size, '\0');
  if (voted_size != 0 && !input.read(voted.data(), voted_size)) return;
  std::lock_guard lock(mutex_);
  current_term_ = term;
  commit_index_ = commit;
  last_applied_ = applied;
  voted_for_ = std::move(voted);
}

/**
 * @brief 持久化 Raft 状态到磁盘
 *
 * 使用临时文件 + 原子重命名确保写入原子性
 */
void ReplicationCoordinator::PersistLocked() const {
  if (state_path_.empty()) return;
  std::error_code error;
  std::filesystem::create_directories(state_path_.parent_path(), error);
  const auto temporary = state_path_.string() + ".tmp";
  std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
  const std::uint32_t magic = 0x31544652u;
  const auto voted_size = static_cast<std::uint32_t>(voted_for_.size());
  output.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
  output.write(reinterpret_cast<const char*>(&current_term_), sizeof(current_term_));
  output.write(reinterpret_cast<const char*>(&commit_index_), sizeof(commit_index_));
  output.write(reinterpret_cast<const char*>(&last_applied_), sizeof(last_applied_));
  output.write(reinterpret_cast<const char*>(&voted_size), sizeof(voted_size));
  output.write(voted_for_.data(), voted_for_.size());
  output.flush();
  output.close();
  if (output) {
    std::filesystem::remove(state_path_, error);
    error.clear();
    std::filesystem::rename(temporary, state_path_, error);
  }
}

/**
 * @brief 获取当前节点角色
 *
 * @return 当前的副本角色
 */
ReplicaRole ReplicationCoordinator::role() const {
  std::lock_guard lock(mutex_);
  return role_;
}

/**
 * @brief 获取节点 ID
 *
 * @return 节点 ID
 */
std::string ReplicationCoordinator::nodeId() const {
  return node_id_;
}

/**
 * @brief 获取当前 Leader ID
 *
 * @return Leader 节点 ID，无 Leader 时返回空字符串
 */
std::string ReplicationCoordinator::leaderId() const {
  std::lock_guard lock(mutex_);
  return leader_id_;
}

/**
 * @brief 获取当前任期
 *
 * @return 任期编号
 */
std::uint64_t ReplicationCoordinator::term() const {
  std::lock_guard lock(mutex_);
  return current_term_;
}

/**
 * @brief 获取最后日志索引（所有分区的最大本地偏移量）
 *
 * @return 日志索引
 */
std::uint64_t ReplicationCoordinator::lastLogIndex() const {
  std::lock_guard lock(mutex_);
  std::uint64_t result = local_offset_;
  for (const auto& [partition, state] : partition_states_)
    result = std::max(result, state.local_offset);
  return result;
}

/**
 * @brief 获取最后日志任期（所有分区的最大日志任期）
 *
 * @return 日志任期
 */
std::uint64_t ReplicationCoordinator::lastLogTerm() const {
  std::lock_guard lock(mutex_);
  std::uint64_t result = 0;
  for (const auto& [partition, state] : partition_states_)
    result = std::max(result, state.last_log_term);
  return result;
}

/**
 * @brief 获取全局提交索引（所有分区的最大提交索引）
 *
 * @return 已提交的最高日志索引
 */
std::uint64_t ReplicationCoordinator::commitIndex() const {
  std::lock_guard lock(mutex_);
  std::uint64_t result = 0;
  for (const auto& [key, state] : partition_states_) result = std::max(result, state.commit_index);
  return result;
}

/**
 * @brief 获取指定分区的提交索引
 *
 * @param partition 分区键
 * @return 已提交的最高日志索引
 */
std::uint64_t ReplicationCoordinator::commitIndex(const PartitionKey& partition) const {
  std::lock_guard lock(mutex_);
  const auto it = partition_states_.find(partition);
  return it == partition_states_.end() ? 0 : it->second.commit_index;
}

/**
 * @brief 获取最后应用的索引
 *
 * @return 已应用到状态机的最高日志索引
 */
std::uint64_t ReplicationCoordinator::lastApplied() const {
  std::lock_guard lock(mutex_);
  return last_applied_;
}

/**
 * @brief 获取当前投票给的节点 ID
 *
 * @return 当前任期投票的节点 ID
 */
std::string ReplicationCoordinator::votedFor() const {
  std::lock_guard lock(mutex_);
  return voted_for_;
}

/**
 * @brief 计算多数派数量
 *
 * @return 多数派所需的最小节点数
 */
std::size_t ReplicationCoordinator::Majority() const {
  return (replicas_.size() + 1) / 2 + 1;
}

/**
 * @brief 检查是否可以服务写请求
 *
 * Leader 必须达到多数派健康才能接受写入。
 *
 * @return true 可以写入；false 不能写入
 */
bool ReplicationCoordinator::CanServeWrites() const {
  std::lock_guard lock(mutex_);
  if (role_ != ReplicaRole::kLeader) return false;
  if (replicas_.empty()) return true;
  std::size_t alive = 1;
  const auto now = std::chrono::steady_clock::now();
  for (const auto& [id, replica] : replicas_)
    if (Healthy(replica, now)) ++alive;
  return alive >= Majority();
}

/**
 * @brief 设置 Leader 节点
 *
 * @param node_id Leader 节点 ID
 */
void ReplicationCoordinator::SetLeader(std::string node_id) {
  std::lock_guard lock(mutex_);
  leader_id_ = std::move(node_id);
  role_ = leader_id_ == node_id_ ? ReplicaRole::kLeader : ReplicaRole::kFollower;
  votes_.clear();
  PersistLocked();
}

/**
 * @brief 注册副本节点
 *
 * @param node_id 副本节点 ID
 */
void ReplicationCoordinator::RegisterReplica(std::string node_id) {
  if (node_id.empty() || node_id == node_id_) return;
  std::lock_guard lock(mutex_);
  replicas_.try_emplace(std::move(node_id));
}

/**
 * @brief 移除副本节点
 *
 * @param node_id 副本节点 ID
 */
void ReplicationCoordinator::RemoveReplica(const std::string& node_id) {
  std::lock_guard lock(mutex_);
  replicas_.erase(node_id);
  for (auto& [partition, progress] : partition_replicas_) progress.erase(node_id);
}

/**
 * @brief 观察心跳（遗留版本，用于向后兼容）
 *
 * @param node_id 副本节点 ID
 * @param replicated_offset 已复制的 offset
 * @param now 当前时间
 */
void ReplicationCoordinator::ObserveHeartbeat(const std::string& node_id,
                                              std::uint64_t replicated_offset,
                                              std::chrono::steady_clock::time_point now) {
  ObserveHeartbeat(kLegacyPartition, node_id, replicated_offset, now);
}

/**
 * @brief 观察分区心跳
 *
 * 更新副本的复制进度和健康状态。
 *
 * @param partition 分区键
 * @param node_id 副本节点 ID
 * @param replicated_offset 已复制的 offset
 * @param now 当前时间
 */
void ReplicationCoordinator::ObserveHeartbeat(const PartitionKey& partition,
                                              const std::string& node_id,
                                              std::uint64_t replicated_offset,
                                              std::chrono::steady_clock::time_point now) {
  if (node_id.empty() || node_id == node_id_) return;
  std::lock_guard lock(mutex_);
  const auto membership = replicas_.find(node_id);
  if (membership == replicas_.end()) return;
  auto& replica = membership->second;
  replica.node_id = node_id;
  replica.replicated_offset = std::max(replica.replicated_offset, replicated_offset);
  replica.last_heartbeat = now;
  replica.healthy = true;
  replica.term = current_term_;
  auto& partition_replica = partition_replicas_[partition][node_id];
  partition_replica.node_id = node_id;
  partition_replica.replicated_offset =
      std::max(partition_replica.replicated_offset, replicated_offset);
  partition_replica.last_heartbeat = now;
  partition_replica.healthy = true;
  partition_replica.term = current_term_;
}

/**
 * @brief 观察追加操作（遗留版本）
 *
 * @param term 任期
 * @param leader_id Leader ID
 * @param replicated_offset 已复制的 offset
 * @param leader_commit Leader 的提交索引
 * @param now 当前时间
 * @return true 追加成功；false 任期不匹配
 */
bool ReplicationCoordinator::ObserveAppend(std::uint64_t term, const std::string& leader_id,
                                           std::uint64_t replicated_offset,
                                           std::uint64_t leader_commit,
                                           std::chrono::steady_clock::time_point now) {
  return ObserveAppend(kLegacyPartition, term, leader_id, replicated_offset, leader_commit, now);
}

/**
 * @brief 观察分区追加操作
 *
 * 接收 Leader 的追加确认，更新提交索引。
 *
 * @param partition 分区键
 * @param term 任期
 * @param leader_id Leader ID
 * @param replicated_offset 已复制的 offset
 * @param leader_commit Leader 的提交索引
 * @param now 当前时间
 * @return true 追加成功；false 任期不匹配
 */
bool ReplicationCoordinator::ObserveAppend(const PartitionKey& partition, std::uint64_t term,
                                           const std::string& leader_id,
                                           std::uint64_t replicated_offset,
                                           std::uint64_t leader_commit,
                                           std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  if (term < current_term_ || leader_id.empty()) return false;
  if (leader_id != node_id_ && replicas_.find(leader_id) == replicas_.end()) return false;
  // 更新任期
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
    PersistLocked();
  }
  leader_id_ = leader_id;
  role_ = leader_id == node_id_ ? ReplicaRole::kLeader : ReplicaRole::kFollower;
  // 更新分区状态
  auto& state = partition_states_[partition];
  state.commit_index = std::min(std::max(state.commit_index, leader_commit), replicated_offset);
  commit_index_ = std::max(commit_index_, state.commit_index);
  last_applied_ = std::max(last_applied_, state.commit_index);
  PersistLocked();
  // 更新副本状态
  if (leader_id != node_id_) {
    const auto membership = replicas_.find(leader_id);
    auto& replica = membership->second;
    replica.node_id = leader_id;
    replica.last_heartbeat = now;
    replica.healthy = true;
    replica.term = term;
    auto& partition_replica = partition_replicas_[partition][leader_id];
    partition_replica = replica;
    partition_replica.replicated_offset = replicated_offset;
  }
  return true;
}

/**
 * @brief 开始选举
 *
 * 递增任期，切换为 Candidate，投给自己。
 *
 * @return 选举结果，包含新的任期
 */
ElectionResult ReplicationCoordinator::BeginElection() {
  std::lock_guard lock(mutex_);
  ++current_term_;
  role_ = ReplicaRole::kCandidate;
  leader_id_.clear();
  voted_for_ = node_id_;
  votes_.clear();
  votes_.insert(node_id_);
  PersistLocked();
  return {current_term_, node_id_};
}

/**
 * @brief 开始预投票
 *
 * 预投票不会递增任期，用于检测是否有可能赢得选举。
 *
 * @return 预投票结果
 */
ElectionResult ReplicationCoordinator::BeginPreVote() const {
  std::lock_guard lock(mutex_);
  return {current_term_ + 1, node_id_};
}

/**
 * @brief 观察预投票结果
 *
 * @param term 预投票任期
 * @param voter_id 投票者 ID
 * @param granted 是否授予投票
 * @return true 获得多数预投票；false 未获得多数
 */
bool ReplicationCoordinator::ObservePreVote(std::uint64_t term, const std::string& voter_id,
                                            bool granted) {
  std::lock_guard lock(mutex_);
  if (term != current_term_ + 1 || !granted || voter_id.empty() ||
      (voter_id != node_id_ && replicas_.find(voter_id) == replicas_.end()))
    return false;
  pre_votes_.insert(voter_id);
  return pre_votes_.size() >= Majority();
}

/**
 * @brief 请求投票（简化版本）
 *
 * @param term 请求者的任期
 * @param candidate_id 候选者 ID
 * @return true 授予投票；false 拒绝投票
 */
bool ReplicationCoordinator::RequestVote(std::uint64_t term, const std::string& candidate_id) {
  return RequestVote(term, candidate_id, 0, 0);
}

/**
 * @brief 请求投票（完整版本）
 *
 * 检查任期、日志完整性和投票限制。
 *
 * @param term 请求者的任期
 * @param candidate_id 候选者 ID
 * @param candidate_last_log_index 候选者最后日志索引
 * @param candidate_last_log_term 候选者最后日志任期
 * @return true 授予投票；false 拒绝投票
 */
bool ReplicationCoordinator::RequestVote(std::uint64_t term, const std::string& candidate_id,
                                         std::uint64_t candidate_last_log_index,
                                         std::uint64_t candidate_last_log_term) {
  std::lock_guard lock(mutex_);
  if (candidate_id.empty() || term < current_term_ ||
      (candidate_id != node_id_ && replicas_.find(candidate_id) == replicas_.end()))
    return false;
  // 获取本地日志信息
  std::uint64_t local_last_log_index = local_offset_;
  std::uint64_t local_last_log_term = 0;
  for (const auto& [partition, state] : partition_states_) {
    local_last_log_index = std::max(local_last_log_index, state.local_offset);
    local_last_log_term = std::max(local_last_log_term, state.last_log_term);
  }
  // 日志完整性检查：候选者日志必须至少和本地一样新
  if (candidate_last_log_term < local_last_log_term ||
      (candidate_last_log_term == local_last_log_term &&
       candidate_last_log_index < local_last_log_index))
    return false;
  // 更新任期
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
    role_ = ReplicaRole::kFollower;
    leader_id_.clear();
  }
  // 检查投票限制：每个任期只能投一票
  if (!voted_for_.empty() && voted_for_ != candidate_id) return false;
  voted_for_ = candidate_id;
  PersistLocked();
  return true;
}

/**
 * @brief 请求预投票
 *
 * 预投票不修改状态，只检查是否有资格参选。
 *
 * @param term 预投票任期
 * @param candidate_id 候选者 ID
 * @param candidate_last_log_index 候选者最后日志索引
 * @param candidate_last_log_term 候选者最后日志任期
 * @return true 授予预投票；false 拒绝预投票
 */
bool ReplicationCoordinator::RequestPreVote(std::uint64_t term, const std::string& candidate_id,
                                            std::uint64_t candidate_last_log_index,
                                            std::uint64_t candidate_last_log_term) const {
  std::lock_guard lock(mutex_);
  if (candidate_id.empty() || term < current_term_ ||
      (candidate_id != node_id_ && replicas_.find(candidate_id) == replicas_.end()))
    return false;
  // 获取本地日志信息
  std::uint64_t local_index = local_offset_;
  std::uint64_t local_term = 0;
  for (const auto& [partition, state] : partition_states_) {
    local_index = std::max(local_index, state.local_offset);
    local_term = std::max(local_term, state.last_log_term);
  }
  // 日志完整性检查
  return candidate_last_log_term > local_term ||
         (candidate_last_log_term == local_term && candidate_last_log_index >= local_index);
}

/**
 * @brief 观察投票结果
 *
 * 达到多数派后晋升为 Leader。
 *
 * @param term 任期
 * @param voter_id 投票者 ID
 * @param granted 是否授予投票
 * @return true 获得多数投票；false 未获得多数
 */
bool ReplicationCoordinator::ObserveVote(std::uint64_t term, const std::string& voter_id,
                                         bool granted) {
  std::lock_guard lock(mutex_);
  if (term != current_term_ || role_ != ReplicaRole::kCandidate || !granted || voter_id.empty())
    return false;
  if (voter_id != node_id_ && replicas_.find(voter_id) == replicas_.end()) return false;
  votes_.insert(voter_id);
  // 检查是否达到多数派
  if (votes_.size() >= Majority()) {
    role_ = ReplicaRole::kLeader;
    leader_id_ = node_id_;
    voted_for_ = node_id_;
    PersistLocked();
    return true;
  }
  return false;
}

/**
 * @brief 推进提交索引（遗留版本）
 *
 * @param offset 新的提交索引
 * @return true 推进成功；false 偏移量无效
 */
bool ReplicationCoordinator::AdvanceCommit(std::uint64_t offset) {
  return AdvanceCommit(kLegacyPartition, offset);
}

/**
 * @brief 推进分区提交索引
 *
 * 检查是否达到多数派确认，然后推进提交索引。
 *
 * @param partition 分区键
 * @param offset 新的提交索引
 * @return true 推进成功；false 偏移量无效
 */
bool ReplicationCoordinator::AdvanceCommit(const PartitionKey& partition, std::uint64_t offset) {
  std::lock_guard lock(mutex_);
  if (role_ != ReplicaRole::kLeader) return false;
  auto& state = partition_states_[partition];
  if (offset > state.local_offset || state.last_log_term != current_term_) return false;
  // 计算确认数量
  std::size_t acknowledgements = 1;
  const auto now = std::chrono::steady_clock::now();
  const auto it = partition_replicas_.find(partition);
  if (it != partition_replicas_.end()) {
    for (const auto& [id, replica] : it->second)
      if (Healthy(replica, now) && replica.replicated_offset >= offset) ++acknowledgements;
  }
  // 检查是否达到多数派
  if (acknowledgements < Majority()) return false;
  state.commit_index = std::max(state.commit_index, offset);
  state.last_applied = state.commit_index;
  commit_index_ = std::max(commit_index_, state.commit_index);
  last_applied_ = std::max(last_applied_, state.last_applied);
  PersistLocked();
  return true;
}

/**
 * @brief 记录本地偏移量（遗留版本）
 *
 * @param offset 本地写入的 offset
 */
void ReplicationCoordinator::RecordLocalOffset(std::uint64_t offset) {
  RecordLocalOffset(kLegacyPartition, offset);
}

/**
 * @brief 记录分区本地偏移量
 *
 * @param partition 分区键
 * @param offset 本地写入的 offset
 */
void ReplicationCoordinator::RecordLocalOffset(const PartitionKey& partition,
                                               std::uint64_t offset) {
  std::lock_guard lock(mutex_);
  auto& state = partition_states_[partition];
  state.local_offset = std::max(state.local_offset, offset);
  state.last_log_term = current_term_;
}

/**
 * @brief 检查日志是否匹配
 *
 * 用于 Follower 验证 Leader 的 AppendEntries 请求。
 *
 * @param partition 分区键
 * @param prev_log_index 前一条日志的索引
 * @param prev_log_term 前一条日志的任期
 * @return true 日志匹配；false 不匹配
 */
bool ReplicationCoordinator::LogMatches(const PartitionKey& partition,
                                        std::uint64_t prev_log_index,
                                        std::uint64_t prev_log_term) const {
  std::lock_guard lock(mutex_);
  const auto it = partition_states_.find(partition);
  const auto local_index = it == partition_states_.end() ? 0 : it->second.local_offset;
  const auto local_term = it == partition_states_.end() ? 0 : it->second.last_log_term;
  return prev_log_index == 0 ||
         (prev_log_index == local_index && prev_log_term == local_term);
}

/**
 * @brief 获取下一个日志索引
 *
 * @param partition 分区键
 * @return 下一个要追加的日志索引
 */
std::uint64_t ReplicationCoordinator::NextLogIndex(const PartitionKey& partition) const {
  std::lock_guard lock(mutex_);
  const auto it = partition_states_.find(partition);
  return it == partition_states_.end() ? 0 : it->second.local_offset + 1;
}

/**
 * @brief 观察心跳轮次
 *
 * 连续 3 轮未收到多数派响应则降级为 Follower。
 *
 * @param majority_responded 是否收到多数派响应
 * @return true 多数派响应；false 未收到多数派响应
 */
bool ReplicationCoordinator::ObserveHeartbeatRound(bool majority_responded) {
  std::lock_guard lock(mutex_);
  if (role_ != ReplicaRole::kLeader) return false;
  if (majority_responded) {
    missed_heartbeat_rounds_ = 0;
    return true;
  }
  if (++missed_heartbeat_rounds_ < 3) return true;
  role_ = ReplicaRole::kFollower;
  leader_id_.clear();
  votes_.clear();
  return false;
}

/**
 * @brief 检查副本是否健康
 *
 * 健康条件：标记为健康且心跳未超时。
 *
 * @param replica 副本进度
 * @param now 当前时间
 * @return true 健康；false 不健康
 */
bool ReplicationCoordinator::Healthy(const ReplicaProgress& replica,
                                     std::chrono::steady_clock::time_point now) const {
  return replica.healthy && now - replica.last_heartbeat <= heartbeat_timeout_;
}

/**
 * @brief 检查是否达到指定数量的副本确认（遗留版本）
 *
 * @param offset 要检查的 offset
 * @param count 副本数量
 * @return true 达到指定数量；false 未达到
 */
bool ReplicationCoordinator::IsQuorumReplicated(std::uint64_t offset,
                                                std::size_t count) const {
  return IsQuorumReplicated(kLegacyPartition, offset, count);
}

/**
 * @brief 检查分区是否达到指定数量的副本确认
 *
 * @param partition 分区键
 * @param offset 要检查的 offset
 * @param count 副本数量
 * @return true 达到指定数量；false 未达到
 */
bool ReplicationCoordinator::IsQuorumReplicated(const PartitionKey& partition,
                                                std::uint64_t offset,
                                                std::size_t count) const {
  std::lock_guard lock(mutex_);
  if (count == 0) return false;
  // 计算确认数量
  const auto state = partition_states_.find(partition);
  std::size_t acknowledgements =
      state != partition_states_.end() && state->second.local_offset >= offset ? 1 : 0;
  const auto now = std::chrono::steady_clock::now();
  const auto it = partition_replicas_.find(partition);
  if (it != partition_replicas_.end()) {
    for (const auto& [id, replica] : it->second)
      if (Healthy(replica, now) && replica.replicated_offset >= offset) ++acknowledgements;
  }
  return acknowledgements >= count;
}

/**
 * @brief 选举 Leader（简化版本）
 *
 * 如果当前节点是 Leader 或健康，则返回自身。
 *
 * @param now 当前时间
 * @return 选出的 Leader ID，无合适 Leader 时返回空字符串
 */
std::string ReplicationCoordinator::ElectLeader(std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  if (role_ == ReplicaRole::kLeader) return node_id_;
  return Healthy(ReplicaProgress{node_id_, 0, now, true, current_term_}, now) ? node_id_
                                                                               : std::string{};
}

/**
 * @brief 获取所有副本的快照
 *
 * @param now 当前时间
 * @return 副本进度列表
 */
std::vector<ReplicaProgress> ReplicationCoordinator::Snapshot(
    std::chrono::steady_clock::time_point now) const {
  std::lock_guard lock(mutex_);
  std::vector<ReplicaProgress> result;
  result.reserve(replicas_.size());
  for (const auto& [id, replica] : replicas_) {
    auto copy = replica;
    copy.healthy = Healthy(copy, now);
    result.push_back(std::move(copy));
  }
  std::sort(result.begin(), result.end(),
            [](const auto& left, const auto& right) { return left.node_id < right.node_id; });
  return result;
}

}  // namespace mq::server