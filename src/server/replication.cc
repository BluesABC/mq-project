#include "mq/server/replication.h"

#include <algorithm>
#include <utility>

namespace mq::server {
namespace {

const ReplicationCoordinator::PartitionKey kLegacyPartition = "__legacy__";

}  // namespace

ReplicationCoordinator::ReplicationCoordinator(std::string node_id, ReplicaRole role,
                                               std::chrono::milliseconds timeout)
    : node_id_(std::move(node_id)), heartbeat_timeout_(timeout), role_(role), leader_id_(node_id_) {}

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

std::uint64_t ReplicationCoordinator::lastLogIndex() const {
  std::lock_guard lock(mutex_);
  std::uint64_t result = local_offset_;
  for (const auto& [partition, state] : partition_states_)
    result = std::max(result, state.local_offset);
  return result;
}

std::uint64_t ReplicationCoordinator::lastLogTerm() const {
  std::lock_guard lock(mutex_);
  std::uint64_t result = 0;
  for (const auto& [partition, state] : partition_states_)
    result = std::max(result, state.last_log_term);
  return result;
}

std::uint64_t ReplicationCoordinator::commitIndex() const {
  std::lock_guard lock(mutex_);
  std::uint64_t result = 0;
  for (const auto& [key, state] : partition_states_) result = std::max(result, state.commit_index);
  return result;
}

std::uint64_t ReplicationCoordinator::commitIndex(const PartitionKey& partition) const {
  std::lock_guard lock(mutex_);
  const auto it = partition_states_.find(partition);
  return it == partition_states_.end() ? 0 : it->second.commit_index;
}

std::size_t ReplicationCoordinator::Majority() const {
  return (replicas_.size() + 1) / 2 + 1;
}

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
  for (auto& [partition, progress] : partition_replicas_) progress.erase(node_id);
}

void ReplicationCoordinator::ObserveHeartbeat(const std::string& node_id,
                                              std::uint64_t replicated_offset,
                                              std::chrono::steady_clock::time_point now) {
  ObserveHeartbeat(kLegacyPartition, node_id, replicated_offset, now);
}

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

bool ReplicationCoordinator::ObserveAppend(std::uint64_t term, const std::string& leader_id,
                                           std::uint64_t replicated_offset,
                                           std::uint64_t leader_commit,
                                           std::chrono::steady_clock::time_point now) {
  return ObserveAppend(kLegacyPartition, term, leader_id, replicated_offset, leader_commit, now);
}

bool ReplicationCoordinator::ObserveAppend(const PartitionKey& partition, std::uint64_t term,
                                           const std::string& leader_id,
                                           std::uint64_t replicated_offset,
                                           std::uint64_t leader_commit,
                                           std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  if (term < current_term_ || leader_id.empty()) return false;
  if (leader_id != node_id_ && replicas_.find(leader_id) == replicas_.end()) return false;
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.clear();
  }
  leader_id_ = leader_id;
  role_ = leader_id == node_id_ ? ReplicaRole::kLeader : ReplicaRole::kFollower;
  auto& state = partition_states_[partition];
  state.commit_index = std::min(std::max(state.commit_index, leader_commit), replicated_offset);
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
  return RequestVote(term, candidate_id, 0, 0);
}

bool ReplicationCoordinator::RequestVote(std::uint64_t term, const std::string& candidate_id,
                                         std::uint64_t candidate_last_log_index,
                                         std::uint64_t candidate_last_log_term) {
  std::lock_guard lock(mutex_);
  if (candidate_id.empty() || term < current_term_ ||
      (candidate_id != node_id_ && replicas_.find(candidate_id) == replicas_.end()))
    return false;
  std::uint64_t local_last_log_index = local_offset_;
  std::uint64_t local_last_log_term = 0;
  for (const auto& [partition, state] : partition_states_) {
    local_last_log_index = std::max(local_last_log_index, state.local_offset);
    local_last_log_term = std::max(local_last_log_term, state.last_log_term);
  }
  if (candidate_last_log_term < local_last_log_term ||
      (candidate_last_log_term == local_last_log_term &&
       candidate_last_log_index < local_last_log_index))
    return false;
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
  return AdvanceCommit(kLegacyPartition, offset);
}

bool ReplicationCoordinator::AdvanceCommit(const PartitionKey& partition, std::uint64_t offset) {
  std::lock_guard lock(mutex_);
  if (role_ != ReplicaRole::kLeader) return false;
  auto& state = partition_states_[partition];
  if (offset > state.local_offset || state.last_log_term != current_term_) return false;
  std::size_t acknowledgements = 1;
  const auto now = std::chrono::steady_clock::now();
  const auto it = partition_replicas_.find(partition);
  if (it != partition_replicas_.end()) {
    for (const auto& [id, replica] : it->second)
      if (Healthy(replica, now) && replica.replicated_offset >= offset) ++acknowledgements;
  }
  if (acknowledgements < Majority()) return false;
  state.commit_index = std::max(state.commit_index, offset);
  state.last_applied = state.commit_index;
  return true;
}

void ReplicationCoordinator::RecordLocalOffset(std::uint64_t offset) {
  RecordLocalOffset(kLegacyPartition, offset);
}

void ReplicationCoordinator::RecordLocalOffset(const PartitionKey& partition,
                                               std::uint64_t offset) {
  std::lock_guard lock(mutex_);
  auto& state = partition_states_[partition];
  state.local_offset = std::max(state.local_offset, offset);
  state.last_log_term = current_term_;
}

bool ReplicationCoordinator::Healthy(const ReplicaProgress& replica,
                                     std::chrono::steady_clock::time_point now) const {
  return replica.healthy && now - replica.last_heartbeat <= heartbeat_timeout_;
}

bool ReplicationCoordinator::IsQuorumReplicated(std::uint64_t offset,
                                                std::size_t count) const {
  return IsQuorumReplicated(kLegacyPartition, offset, count);
}

bool ReplicationCoordinator::IsQuorumReplicated(const PartitionKey& partition,
                                                std::uint64_t offset,
                                                std::size_t count) const {
  std::lock_guard lock(mutex_);
  if (count == 0) return false;
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

std::string ReplicationCoordinator::ElectLeader(std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  if (role_ == ReplicaRole::kLeader) return node_id_;
  return Healthy(ReplicaProgress{node_id_, 0, now, true, current_term_}, now) ? node_id_
                                                                               : std::string{};
}

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
