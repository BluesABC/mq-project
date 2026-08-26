#include "mq/server/replication.h"

#include <algorithm>
#include <utility>
namespace mq::server {

// 该协调器只维护复制元数据；消息内容仍由 StorageEngine 保证顺序和持久化。
ReplicationCoordinator::ReplicationCoordinator(std::string node_id, ReplicaRole role,
                                               std::chrono::milliseconds timeout)
    : node_id_(std::move(node_id)),
      heartbeat_timeout_(timeout),
      role_(role),
      leader_id_(node_id_) {}
ReplicaRole ReplicationCoordinator::role() const {
  std::lock_guard lock(mutex_);
  return role_;
}
std::string ReplicationCoordinator::nodeId() const {
  return node_id_;
}
std::string ReplicationCoordinator::leaderId() const {
  std::lock_guard lock(mutex_);
  return leader_id_;
}
std::uint64_t ReplicationCoordinator::term() const {
  std::lock_guard lock(mutex_);
  return current_term_;
}
std::uint64_t ReplicationCoordinator::commitIndex() const {
  std::lock_guard lock(mutex_);
  return commit_index_;
}
std::uint64_t ReplicationCoordinator::lastApplied() const {
  std::lock_guard lock(mutex_);
  return last_applied_;
}
std::size_t ReplicationCoordinator::Majority() const {
  return (replicas_.size() + 1) / 2 + 1;
}
bool ReplicationCoordinator::CanServeWrites() const {
  // Leader 失去多数派时停止接收写入，避免产生无法提交的新数据。
  std::lock_guard lock(mutex_);
  if (role_ != ReplicaRole::kLeader) return false;
  if (replicas_.empty()) return true;
  std::size_t alive = 1;
  const auto now = std::chrono::steady_clock::now();
  for (const auto& [id, replica] : replicas_)
    if (Healthy(replica, now)) ++alive;
  return alive >= Majority();
}
void ReplicationCoordinator::SetLeader(std::string node_id) {
  std::lock_guard lock(mutex_);
  leader_id_ = std::move(node_id);
  role_ = leader_id_ == node_id_ ? ReplicaRole::kLeader : ReplicaRole::kFollower;
  votes_.clear();
}
void ReplicationCoordinator::RegisterReplica(std::string node_id) {
  if (node_id.empty() || node_id == node_id_) return;
  std::lock_guard lock(mutex_);
  replicas_.try_emplace(std::move(node_id));
}
void ReplicationCoordinator::RemoveReplica(const std::string& node_id) {
  std::lock_guard lock(mutex_);
  replicas_.erase(node_id);
}
void ReplicationCoordinator::ObserveHeartbeat(const std::string& node_id, std::uint64_t offset,
                                              std::chrono::steady_clock::time_point now) {
  if (node_id.empty() || node_id == node_id_) return;
  std::lock_guard lock(mutex_);
  auto& replica = replicas_[node_id];
  replica.node_id = node_id;
  replica.replicated_offset = std::max(replica.replicated_offset, offset);
  replica.last_heartbeat = now;
  replica.healthy = true;
  replica.term = current_term_;
}
bool ReplicationCoordinator::ObserveAppend(std::uint64_t term, const std::string& leader_id,
                                           std::uint64_t offset, std::uint64_t leader_commit,
                                           std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  if (term < current_term_) return false;
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
  }
  leader_id_ = leader_id;
  role_ = leader_id == node_id_ ? ReplicaRole::kLeader : ReplicaRole::kFollower;
  commit_index_ = std::min(std::max(commit_index_, leader_commit), offset);
  if (!leader_id.empty() && leader_id != node_id_) {
    auto& replica = replicas_[leader_id];
    replica.node_id = leader_id;
    replica.replicated_offset = std::max(replica.replicated_offset, offset);
    replica.last_heartbeat = now;
    replica.healthy = true;
    replica.term = term;
  }
  return true;
}
ElectionResult ReplicationCoordinator::BeginElection() {
  std::lock_guard lock(mutex_);
  ++current_term_;
  role_ = ReplicaRole::kCandidate;
  leader_id_.clear();
  voted_for_ = node_id_;
  votes_.clear();
  votes_.insert(node_id_);
  return {current_term_, node_id_};
}
bool ReplicationCoordinator::RequestVote(std::uint64_t term, const std::string& candidate_id) {
  std::lock_guard lock(mutex_);
  if (candidate_id.empty() || term < current_term_) return false;
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
    role_ = ReplicaRole::kFollower;
    leader_id_.clear();
  }
  if (!voted_for_.empty() && voted_for_ != candidate_id) return false;
  voted_for_ = candidate_id;
  return true;
}
bool ReplicationCoordinator::ObserveVote(std::uint64_t term, const std::string& voter_id,
                                         bool granted) {
  std::lock_guard lock(mutex_);
  if (term != current_term_ || role_ != ReplicaRole::kCandidate || !granted || voter_id.empty())
    return false;
  if (voter_id != node_id_ && replicas_.find(voter_id) == replicas_.end()) return false;
  votes_.insert(voter_id);
  if (votes_.size() >= Majority()) {
    role_ = ReplicaRole::kLeader;
    leader_id_ = node_id_;
    voted_for_ = node_id_;
    return true;
  }
  return false;
}
bool ReplicationCoordinator::AdvanceCommit(std::uint64_t offset) {
  std::lock_guard lock(mutex_);
  if (role_ != ReplicaRole::kLeader || offset > local_offset_) return false;
  std::size_t a = 1;
  const auto now = std::chrono::steady_clock::now();
  for (const auto& [id, r] : replicas_)
    if (Healthy(r, now) && r.replicated_offset >= offset) ++a;
  if (a < Majority()) return false;
  commit_index_ = std::max(commit_index_, offset);
  last_applied_ = commit_index_;
  return true;
}
void ReplicationCoordinator::RecordLocalOffset(std::uint64_t offset) {
  std::lock_guard lock(mutex_);
  local_offset_ = std::max(local_offset_, offset);
}
bool ReplicationCoordinator::Healthy(const ReplicaProgress& replica,
                                     std::chrono::steady_clock::time_point now) const {
  return replica.healthy && now - replica.last_heartbeat <= heartbeat_timeout_;
}
bool ReplicationCoordinator::IsQuorumReplicated(std::uint64_t offset, std::size_t count) const {
  std::lock_guard lock(mutex_);
  if (count == 0) return false;
  std::size_t a = local_offset_ >= offset ? 1 : 0;
  const auto now = std::chrono::steady_clock::now();
  for (const auto& [id, r] : replicas_)
    if (Healthy(r, now) && r.replicated_offset >= offset) ++a;
  return a >= count;
}
std::string ReplicationCoordinator::ElectLeader(std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  if (role_ == ReplicaRole::kLeader) return node_id_;
  return Healthy(ReplicaProgress{node_id_, local_offset_, now, true, current_term_}, now)
             ? node_id_
             : std::string{};
}
std::vector<ReplicaProgress> ReplicationCoordinator::Snapshot(
    std::chrono::steady_clock::time_point now) const {
  std::lock_guard lock(mutex_);
  std::vector<ReplicaProgress> result;
  result.reserve(replicas_.size());
  for (const auto& [id, r] : replicas_) {
    auto copy = r;
    copy.healthy = Healthy(copy, now);
    result.push_back(std::move(copy));
  }
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a.node_id < b.node_id; });
  return result;
}
}  // namespace mq::server
