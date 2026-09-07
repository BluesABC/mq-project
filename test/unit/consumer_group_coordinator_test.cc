#include "mq/core/consumer_group_coordinator.h"

#include <cassert>
#include <chrono>

int main() {
  using Coordinator = mq::core::ConsumerGroupCoordinator;
  using namespace std::chrono_literals;
  const auto start = Coordinator::Clock::now();
  Coordinator coordinator;
  coordinator.SetPartitions("orders", 4);
  coordinator.SetPartitions("payments", 2);
  assert(coordinator.Join("group", "member-a", {"orders", "payments"}, start));
  assert(coordinator.Join("group", "member-b", {"orders", "payments"}, start));
  std::vector<mq::core::GroupAssignment> first;
  std::vector<mq::core::GroupAssignment> second;
  assert(coordinator.Sync("group", "member-a", &first));
  assert(coordinator.Sync("group", "member-b", &second));
  assert(first.size() == 3 && second.size() == 3);
  assert(first[0].topic == "orders" && first[0].partition == 0);
  assert(second[0].topic == "orders" && second[0].partition == 1);
  assert(coordinator.Heartbeat("group", "member-a", start + 29s));
  const auto expired = coordinator.Expire(start + 31s);
  assert(expired.size() == 1 && expired[0] == "group\x1fmember-b");
  assert(coordinator.Assignment("group", "member-a").size() == 6);
  assert(!coordinator.Heartbeat("group", "member-b", start + 31s));
  return 0;
}
