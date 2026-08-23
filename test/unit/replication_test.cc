#include <cassert>
#include <chrono>

#include "mq/server/replication.h"

int main() {
  using namespace std::chrono_literals;
  const auto start = std::chrono::steady_clock::now();
  mq::server::ReplicationCoordinator coordinator("node-a", mq::server::ReplicaRole::kLeader, 5s);
  coordinator.RegisterReplica("node-b");
  coordinator.RegisterReplica("node-c");
  coordinator.RecordLocalOffset(10);
  coordinator.ObserveHeartbeat("node-b", 10, start);
  coordinator.ObserveHeartbeat("node-c", 9, start);
  assert(coordinator.IsQuorumReplicated(10, 2));
  assert(!coordinator.IsQuorumReplicated(10, 3));
  assert(coordinator.ElectLeader(start) == "node-a");
  assert(coordinator.Snapshot(start).size() == 2);
  assert(coordinator.ElectLeader(start + 6s) == "node-a");
  coordinator.SetLeader("node-b");
  assert(coordinator.role() == mq::server::ReplicaRole::kFollower);
  coordinator.SetLeader("node-a");
  assert(coordinator.role() == mq::server::ReplicaRole::kLeader);
  coordinator.RemoveReplica("node-c");
  assert(coordinator.Snapshot(start).size() == 1);
  return 0;
}
