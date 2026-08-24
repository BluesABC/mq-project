#include <cassert>
#include <filesystem>
#include <string>
#include <vector>

#include "mq/client/mq_client.h"
#include "mq/network/tcp_server.h"
#include "mq/server/broker.h"

int main() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_client_test";
  std::error_code ec; std::filesystem::remove_all(root, ec);
  mq::server::Broker broker(root); assert(broker.Open());
  mq::network::TcpServer server(0, 2, [&broker](const mq::protocol::Request& request) { return broker.Handle(request); });
  assert(server.Start());
  mq::client::MqProducer producer; assert(producer.connect("localhost", server.port())); assert(producer.createTopic("client", 1));
  std::vector<mq::client::TopicInfo> topics; assert(producer.listTopics(&topics)); assert(topics.size() == 1 && topics[0].name == "client" && topics[0].partitions == 1);
  std::string metrics; assert(producer.metrics(&metrics)); assert(metrics.find("mq_requests_total ") != std::string::npos);
  mq::client::ProduceResult result; assert(producer.produce("client", "key", "one", mq::client::AckMode::kOne, &result)); assert(result.offset == 0);
  std::vector<mq::client::ProducerMessage> batch{{"key", "two"}, {"key", "three"}}; std::vector<mq::client::ProduceResult> results;
  assert(producer.produceBatch("client", batch, mq::client::AckMode::kOne, &results)); assert(results.size() == 2 && results[0].offset == 1 && results[1].offset == 2);
  mq::client::MqConsumer consumer; assert(consumer.connect("localhost", server.port())); assert(consumer.subscribe("client", "group"));
  auto first = consumer.poll(); assert(first.has_value() && first->value == "one"); assert(consumer.commit(1));
  const auto follower_root = root / "endpoint-follower";
  const auto leader_root = root / "endpoint-leader";
  mq::server::Broker follower(follower_root); assert(follower.Open());
  assert(follower.Handle([&] { mq::protocol::Request request; request.command = mq::protocol::Command::kCreateTopic; request.topic = "failover"; request.payload = std::string("\0\0\0\1", 4); return request; }()).status == mq::protocol::Status::kOk);
  follower.ConfigureReplication("follower", {}, 0, true);
  mq::network::TcpServer follower_server("127.0.0.1", 0, 1, [&follower](const auto& request) { return follower.Handle(request); });
  assert(follower_server.Start());
  mq::server::Broker failover_leader(leader_root); assert(failover_leader.Open());
  mq::protocol::Request failover_topic; failover_topic.command = mq::protocol::Command::kCreateTopic; failover_topic.topic = "failover"; failover_topic.payload = std::string("\0\0\0\1", 4);
  assert(failover_leader.Handle(failover_topic).status == mq::protocol::Status::kOk);
  mq::network::TcpServer leader_server("127.0.0.1", 0, 1, [&failover_leader](const auto& request) { return failover_leader.Handle(request); });
  assert(leader_server.Start());
  mq::client::MqProducer failover_producer;
  assert(failover_producer.connect({{"127.0.0.1", follower_server.port()}, {"127.0.0.1", leader_server.port()}}));
  assert(failover_producer.produce("failover", "k", "v"));
  failover_producer.close(); leader_server.Stop(); follower_server.Stop();
  producer.close(); consumer.close(); server.Stop(); broker.Flush(); std::filesystem::remove_all(root, ec); return 0;
}
