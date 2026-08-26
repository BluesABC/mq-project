#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mq::server {

// 复制协调器只管理角色、任期、位点和投票状态，实际网络传输由 ReplicationClient 完成。
enum class ReplicaRole { kLeader, kFollower, kCandidate };
struct ReplicaProgress {
  std::string node_id;
  std::uint64_t replicated_offset = 0;
  std::chrono::steady_clock::time_point last_heartbeat;
  bool healthy = false;
  std::uint64_t term = 0;
};
struct ElectionResult {
  std::uint64_t term = 0;
  std::string candidate_id;
};
class ReplicationCoordinator {
 public:
  ReplicationCoordinator(std::string node_id, ReplicaRole role = ReplicaRole::kLeader,
                         std::chrono::milliseconds heartbeat_timeout = std::chrono::seconds(10));
  ReplicaRole role() const;
  std::string nodeId() const;
  std::string leaderId() const;
  std::uint64_t term() const;
  std::uint64_t commitIndex() const;
  std::uint64_t lastApplied() const;
  bool CanServeWrites() const;
  void SetLeader(std::string node_id);
  void RegisterReplica(std::string node_id);
  void RemoveReplica(const std::string& node_id);
  void ObserveHeartbeat(
      const std::string& node_id, std::uint64_t replicated_offset,
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  bool ObserveAppend(std::uint64_t term, const std::string& leader_id,
                     std::uint64_t replicated_offset, std::uint64_t leader_commit,
                     std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  ElectionResult BeginElection();
  bool RequestVote(std::uint64_t term, const std::string& candidate_id);
  bool ObserveVote(std::uint64_t term, const std::string& voter_id, bool granted);
  bool AdvanceCommit(std::uint64_t offset);
  void RecordLocalOffset(std::uint64_t offset);
  bool IsQuorumReplicated(std::uint64_t offset, std::size_t replica_count) const;
  std::string ElectLeader(
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  std::vector<ReplicaProgress> Snapshot(
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) const;

 private:
  bool Healthy(const ReplicaProgress& replica, std::chrono::steady_clock::time_point now) const;
  std::size_t Majority() const;
  const std::string node_id_;
  const std::chrono::milliseconds heartbeat_timeout_;
  mutable std::mutex mutex_;
  ReplicaRole role_;
  std::string leader_id_;
  std::string voted_for_;
  std::uint64_t current_term_ = 0;
  std::uint64_t local_offset_ = 0;
  std::uint64_t commit_index_ = 0;
  std::uint64_t last_applied_ = 0;
  std::unordered_set<std::string> votes_;
  std::unordered_map<std::string, ReplicaProgress> replicas_;
};
}  // namespace mq::server
