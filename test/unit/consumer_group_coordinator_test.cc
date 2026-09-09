/**
 * @file consumer_group_coordinator_test.cc
 * @brief 消费者组协调器单元测试
 * 
 * 本文件包含对消费者组协调器功能的单元测试，验证以下功能：
 * 1. 消费者组成员加入(Join)和同步(Sync)
 * 2. 分区分配策略（均匀分配）
 * 3. 心跳(Heartbeat)机制和成员过期(Expire)
 * 4. 分区 assignment 查询
 * 
 * 测试使用模拟时间验证超时逻辑。
 */

#include "mq/core/consumer_group_coordinator.h"

#include <cassert>
#include <chrono>

/**
 * @brief 主函数，运行消费者组协调器单元测试
 * @return 0 表示测试成功
 */
int main() {
  using Coordinator = mq::core::ConsumerGroupCoordinator;
  using namespace std::chrono_literals;
  
  // 初始化协调器和模拟时间
  const auto start = Coordinator::Clock::now();
  Coordinator coordinator;
  
  // 设置主题分区数
  coordinator.SetPartitions("orders", 4);
  coordinator.SetPartitions("payments", 2);
  
  // 测试成员加入
  assert(coordinator.Join("group", "member-a", {"orders", "payments"}, start));
  assert(coordinator.Join("group", "member-b", {"orders", "payments"}, start));
  
  // 测试分区分配同步
  std::vector<mq::core::GroupAssignment> first;
  std::vector<mq::core::GroupAssignment> second;
  assert(coordinator.Sync("group", "member-a", &first));
  assert(coordinator.Sync("group", "member-b", &second));
  
  // 验证分配结果（每个成员分配3个分区）
  assert(first.size() == 3 && second.size() == 3);
  assert(first[0].topic == "orders" && first[0].partition == 0);
  assert(second[0].topic == "orders" && second[0].partition == 1);
  
  // 测试心跳机制（member-a在29秒时心跳正常）
  assert(coordinator.Heartbeat("group", "member-a", start + 29s));
  
  // 测试成员过期（member-b在31秒时过期）
  const auto expired = coordinator.Expire(start + 31s);
  assert(expired.size() == 1 && expired[0] == "group\x1fmember-b");
  
  // 验证member-a的分区assignment（6个分区）
  assert(coordinator.Assignment("group", "member-a").size() == 6);
  
  // 验证过期成员心跳失败
  assert(!coordinator.Heartbeat("group", "member-b", start + 31s));
  
  return 0;
}
