#include "mq/server/replication.h"

#include <cassert>
#include <chrono>

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

  mq::server::ReplicationCoordinator election("node-a", mq::server::ReplicaRole::kFollower, 5s);
  election.RegisterReplica("node-b");
  election.RegisterReplica("node-c");
  const auto round = election.BeginElection();
  assert(round.term == 1);
  assert(!election.ObserveVote(round.term - 1, "node-b", true));
  assert(election.ObserveVote(round.term, "node-b", true));
  assert(election.role() == mq::server::ReplicaRole::kLeader);
  assert(election.term() == 1);
  assert(election.CanServeWrites() == false);
  assert(!election.ObserveAppend(0, "node-b", 1, 1));

  mq::server::ReplicationCoordinator log_guard("node-a");
  log_guard.RegisterReplica("node-b");
  log_guard.RecordLocalOffset(10);
  assert(!log_guard.RequestVote(1, "node-b", 9, 0));
  assert(log_guard.RequestVote(1, "node-b", 10, 1));

  mq::server::ReplicationCoordinator term_guard("node-a");
  term_guard.RegisterReplica("node-b");
  term_guard.RecordLocalOffset(10);
  const auto term_round = term_guard.BeginElection();
  term_guard.ObserveHeartbeat("node-b", 10, start);
  assert(term_guard.ObserveVote(term_round.term, "node-b", true));
  assert(!term_guard.AdvanceCommit(10));
  return 0;
}
