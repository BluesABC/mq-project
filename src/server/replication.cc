#include "mq/server/replication.h"

#include <algorithm>
#include <utility>

namespace mq::server {

ReplicationCoordinator::ReplicationCoordinator(std::string node_id, ReplicaRole role,
                                               std::chrono::milliseconds heartbeat_timeout)
    : node_id_(std::move(node_id)), heartbeat_timeout_(heartbeat_timeout), role_(role), leader_id_(node_id_) {}

ReplicaRole ReplicationCoordinator::role() const {
  std::lock_guard lock(mutex_);
  return role_;
}

std::string ReplicationCoordinator::nodeId() const { return node_id_; }

std::string ReplicationCoordinator::leaderId() const {
  std::lock_guard lock(mutex_);
  return leader_id_;
}

void ReplicationCoordinator::SetLeader(std::string node_id) {
  std::lock_guard lock(mutex_);
  leader_id_ = std::move(node_id);
  role_ = leader_id_ == node_id_ ? ReplicaRole::kLeader : ReplicaRole::kFollower;
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

void ReplicationCoordinator::ObserveHeartbeat(const std::string& node_id, std::uint64_t replicated_offset,
                                              std::chrono::steady_clock::time_point now) {
  if (node_id.empty() || node_id == node_id_) return;
  std::lock_guard lock(mutex_);
  auto& replica = replicas_[node_id];
  replica.node_id = node_id;
  replica.replicated_offset = std::max(replica.replicated_offset, replicated_offset);
  replica.last_heartbeat = now;
  replica.healthy = true;
}

void ReplicationCoordinator::RecordLocalOffset(std::uint64_t offset) {
  std::lock_guard lock(mutex_);
  local_offset_ = std::max(local_offset_, offset);
}

bool ReplicationCoordinator::Healthy(const ReplicaProgress& replica,
                                     std::chrono::steady_clock::time_point now) const {
  return replica.healthy && now - replica.last_heartbeat <= heartbeat_timeout_;
}

bool ReplicationCoordinator::IsQuorumReplicated(std::uint64_t offset, std::size_t replica_count) const {
  std::lock_guard lock(mutex_);
  if (replica_count == 0) return false;
  std::size_t acknowledgements = local_offset_ >= offset ? 1 : 0;
  const auto now = std::chrono::steady_clock::now();
  for (const auto& [id, replica] : replicas_)
    if (Healthy(replica, now) && replica.replicated_offset >= offset) ++acknowledgements;
  return acknowledgements >= replica_count;
}

std::string ReplicationCoordinator::ElectLeader(std::chrono::steady_clock::time_point now) {
  std::lock_guard lock(mutex_);
  std::string elected;
  if (role_ == ReplicaRole::kLeader && Healthy(ReplicaProgress{node_id_, local_offset_, now, true}, now)) elected = node_id_;
  for (const auto& [id, replica] : replicas_)
    if (Healthy(replica, now) && (elected.empty() || id < elected)) elected = id;
  if (!elected.empty()) {
    leader_id_ = elected;
    role_ = elected == node_id_ ? ReplicaRole::kLeader : ReplicaRole::kFollower;
  } else {
    role_ = ReplicaRole::kCandidate;
  }
  return elected;
}

std::vector<ReplicaProgress> ReplicationCoordinator::Snapshot(std::chrono::steady_clock::time_point now) const {
  std::lock_guard lock(mutex_);
  std::vector<ReplicaProgress> result;
  result.reserve(replicas_.size());
  for (const auto& [id, replica] : replicas_) {
    auto copy = replica;
    copy.healthy = Healthy(copy, now);
    result.push_back(std::move(copy));
  }
  std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) { return left.node_id < right.node_id; });
  return result;
}

}  // namespace mq::server
