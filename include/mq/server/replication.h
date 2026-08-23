#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mq::server {

enum class ReplicaRole { kLeader, kFollower, kCandidate };

struct ReplicaProgress {
  std::string node_id;
  std::uint64_t replicated_offset = 0;
  std::chrono::steady_clock::time_point last_heartbeat;
  bool healthy = false;
};

class ReplicationCoordinator {
 public:
  ReplicationCoordinator(std::string node_id, ReplicaRole role = ReplicaRole::kLeader,
                         std::chrono::milliseconds heartbeat_timeout = std::chrono::seconds(10));

  ReplicaRole role() const;
  std::string nodeId() const;
  std::string leaderId() const;
  void SetLeader(std::string node_id);
  void RegisterReplica(std::string node_id);
  void RemoveReplica(const std::string& node_id);
  void ObserveHeartbeat(const std::string& node_id, std::uint64_t replicated_offset,
                        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  void RecordLocalOffset(std::uint64_t offset);
  bool IsQuorumReplicated(std::uint64_t offset, std::size_t replica_count) const;
  std::string ElectLeader(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  std::vector<ReplicaProgress> Snapshot(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) const;

 private:
  bool Healthy(const ReplicaProgress& replica, std::chrono::steady_clock::time_point now) const;

  const std::string node_id_;
  const std::chrono::milliseconds heartbeat_timeout_;
  mutable std::mutex mutex_;
  ReplicaRole role_;
  std::string leader_id_;
  std::uint64_t local_offset_ = 0;
  std::unordered_map<std::string, ReplicaProgress> replicas_;
};

}  // namespace mq::server
