/**
 * @file replication_test.cc
 * @brief 复制协调器单元测试
 * 
 * 本文件包含对复制协调器(ReplicationCoordinator)功能的单元测试，验证以下功能：
 * 1. 集群节点注册和心跳观察
 * 2. Quorum复制确认
 * 3. Leader选举和角色切换
 * 4. 日志匹配(LogMatches)检查
 * 5. 心跳轮次观察
 * 6. 预投票(PreVote)机制
 * 7. 状态持久化和恢复
 * 8. 日志条目完整性保护
 * 
 * 测试使用模拟时间验证超时和选举逻辑。
 */

#include "mq/server/replication.h"

#include <cassert>
#include <chrono>
#include <filesystem>

/**
 * @brief 主函数，运行所有复制协调器单元测试
 * @return 0 表示测试成功
 */
int main() {
  using namespace std::chrono_literals;
  const auto start = std::chrono::steady_clock::now();
  
  // 测试基本集群管理：节点注册、心跳、Quorum确认
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
  
  // 测试角色切换
  coordinator.SetLeader("node-b");
  assert(coordinator.role() == mq::server::ReplicaRole::kFollower);
  coordinator.SetLeader("node-a");
  assert(coordinator.role() == mq::server::ReplicaRole::kLeader);
  
  // 测试节点移除
  coordinator.RemoveReplica("node-c");
  assert(coordinator.Snapshot(start).size() == 1);

  // 测试选举流程
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

  // 测试日志完整性保护（旧任期投票被拒绝）
  mq::server::ReplicationCoordinator log_guard("node-a");
  log_guard.RegisterReplica("node-b");
  log_guard.RecordLocalOffset(10);
  assert(!log_guard.RequestVote(1, "node-b", 9, 0));
  assert(log_guard.RequestVote(1, "node-b", 10, 1));

  // 测试提交保护（心跳观察阻止提交）
  mq::server::ReplicationCoordinator term_guard("node-a");
  term_guard.RegisterReplica("node-b");
  term_guard.RecordLocalOffset(10);
  const auto term_round = term_guard.BeginElection();
  term_guard.ObserveHeartbeat("node-b", 10, start);
  assert(term_guard.ObserveVote(term_round.term, "node-b", true));
  assert(!term_guard.AdvanceCommit(10));

  // 测试状态持久化和恢复
  const auto state_dir = std::filesystem::temp_directory_path() / "mq_raft_replication_test";
  std::filesystem::remove_all(state_dir);
  {
    mq::server::ReplicationCoordinator persisted("node-p", mq::server::ReplicaRole::kFollower,
                                                  5s, state_dir);
    persisted.RegisterReplica("node-q");
    const auto pre_vote = persisted.BeginPreVote();
    assert(pre_vote.term == 1);
    assert(persisted.ObservePreVote(pre_vote.term, "node-p", true) == false);
    assert(persisted.ObservePreVote(pre_vote.term, "node-q", true));
    const auto round = persisted.BeginElection();
    assert(round.term == 1);
    assert(persisted.votedFor() == "node-p");
    persisted.SetLeader("node-p");
    persisted.RecordLocalOffset("topic:0", 4);
    persisted.ObserveHeartbeat("topic:0", "node-q", 4);
  }
  {
    mq::server::ReplicationCoordinator restored("node-p", mq::server::ReplicaRole::kFollower,
                                                 5s, state_dir);
    assert(restored.term() == 1);
    assert(restored.votedFor() == "node-p");
  }
  std::filesystem::remove_all(state_dir);

  // 测试日志匹配检查
  mq::server::ReplicationCoordinator matching("node-a");
  matching.RecordLocalOffset("topic:0", 3);
  assert(matching.LogMatches("topic:0", 0, 0));
  assert(matching.LogMatches("topic:0", 3, matching.lastLogTerm()));
  assert(!matching.LogMatches("topic:0", 2, matching.lastLogTerm()));
  
  // 测试心跳轮次观察
  assert(matching.ObserveHeartbeatRound(false));
  assert(matching.ObserveHeartbeatRound(false));
  assert(!matching.ObserveHeartbeatRound(false));
  assert(matching.role() == mq::server::ReplicaRole::kFollower);
  
  return 0;
}
