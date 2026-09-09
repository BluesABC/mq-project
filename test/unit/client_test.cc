/**
 * @file client_test.cc
 * @brief 客户端SDK单元测试
 * 
 * 本文件包含对消息队列客户端SDK功能的单元测试，验证以下功能：
 * 1. 生产者(Producer)连接、认证、创建主题
 * 2. 消息生产（单条、批量、fire-and-forget模式）
 * 3. 生产者指标(Metrics)查询
 * 4. 消费者(Consumer)连接、订阅、轮询消息
 * 5. 消费者偏移量提交与恢复
 * 6. 多端点故障转移(Failover)功能
 * 7. 认证与授权机制
 * 
 * 测试使用内嵌的Broker和TcpServer模拟服务端环境。
 */

#include <cassert>
#include <filesystem>
#include <string>
#include <vector>

#include "mq/client/mq_client.h"
#include "mq/network/tcp_server.h"
#include "mq/server/broker.h"

/**
 * @brief 主函数，运行所有客户端SDK单元测试
 * @return 0 表示测试成功
 */
int main() {
  // 创建临时目录和Broker实例
  const auto root = std::filesystem::temp_directory_path() / "mq_project_client_test";
  std::error_code ec;
  std::filesystem::remove_all(root, ec);
  mq::server::Broker broker(root);
  assert(broker.Open());
  broker.ConfigureClientAuth("client-token");
  
  // 启动TcpServer模拟Broker服务
  mq::network::TcpServer server(
      0, 2, [&broker](const mq::protocol::Request& request) { return broker.Handle(request); });
  assert(server.Start());
  
  // 测试生产者功能
  mq::client::MqProducer producer;
  producer.setAuthToken("client-token");
  assert(producer.connect("localhost", server.port()));
  assert(producer.createTopic("client", 1));
  
  // 测试主题列表查询
  std::vector<mq::client::TopicInfo> topics;
  assert(producer.listTopics(&topics));
  assert(topics.size() == 1 && topics[0].name == "client" && topics[0].partitions == 1);
  
  // 测试指标查询
  std::string metrics;
  assert(producer.metrics(&metrics));
  assert(metrics.find("mq_requests_total ") != std::string::npos);
  
  // 测试单条消息生产
  mq::client::ProduceResult result;
  assert(producer.produce("client", "key", "one", mq::client::AckMode::kOne, &result));
  assert(result.offset == 0);
  
  // 测试批量消息生产
  std::vector<mq::client::ProducerMessage> batch{{"key", "two"}, {"key", "three"}};
  std::vector<mq::client::ProduceResult> results;
  assert(producer.produceBatch("client", batch, mq::client::AckMode::kOne, &results));
  assert(results.size() == 2 && results[0].offset == 1 && results[1].offset == 2);
  
  // 测试fire-and-forget模式
  assert(producer.produce("client", "key", "fire-and-forget", mq::client::AckMode::kZero));
  assert(producer.flush());
  
  // 测试不支持的AckMode（kAll）
  assert(!producer.produce("client", "key", "quorum-required", mq::client::AckMode::kAll));
  
  // 测试消费者功能
  mq::client::MqConsumer consumer;
  consumer.setAuthToken("client-token");
  assert(consumer.connect("localhost", server.port()));
  assert(consumer.subscribe("client", "group"));
  
  // 测试消息轮询和偏移量提交
  auto first = consumer.poll();
  assert(first.has_value() && first->value == "one");
  assert(consumer.commit(1));
  
  // 测试消费者恢复（从提交的偏移量继续消费）
  mq::client::MqConsumer resumed;
  resumed.setAuthToken("client-token");
  assert(resumed.connect("localhost", server.port()));
  assert(resumed.subscribe("client", "group"));
  auto resumed_message = resumed.poll();
  assert(resumed_message.has_value() && resumed_message->value == "two");
  
  // 继续消费剩余消息
  auto second = consumer.poll();
  auto third = consumer.poll();
  assert(second.has_value() && second->value == "two");
  assert(third.has_value() && third->value == "three");
  
  // 测试多端点故障转移功能
  const auto follower_root = root / "endpoint-follower";
  const auto leader_root = root / "endpoint-leader";
  mq::server::Broker follower(follower_root);
  assert(follower.Open());
  assert(follower
             .Handle([&] {
               mq::protocol::Request request;
               request.command = mq::protocol::Command::kCreateTopic;
               request.topic = "failover";
               request.payload = std::string("\0\0\0\1", 4);
               return request;
             }())
             .status == mq::protocol::Status::kOk);
  follower.ConfigureReplication("follower", {}, 0, true);
  mq::network::TcpServer follower_server(
      "127.0.0.1", 0, 1, [&follower](const auto& request) { return follower.Handle(request); });
  assert(follower_server.Start());
  
  mq::server::Broker failover_leader(leader_root);
  assert(failover_leader.Open());
  mq::protocol::Request failover_topic;
  failover_topic.command = mq::protocol::Command::kCreateTopic;
  failover_topic.topic = "failover";
  failover_topic.payload = std::string("\0\0\0\1", 4);
  assert(failover_leader.Handle(failover_topic).status == mq::protocol::Status::kOk);
  mq::network::TcpServer leader_server("127.0.0.1", 0, 1, [&failover_leader](const auto& request) {
    return failover_leader.Handle(request);
  });
  assert(leader_server.Start());
  
  // 测试故障转移生产者
  mq::client::MqProducer failover_producer;
  assert(failover_producer.connect(
      {{"127.0.0.1", follower_server.port()}, {"127.0.0.1", leader_server.port()}}));
  assert(failover_producer.produce("failover", "k", "v"));
  
  // 清理资源
  failover_producer.close();
  leader_server.Stop();
  follower_server.Stop();
  producer.close();
  consumer.close();
  server.Stop();
  broker.Flush();
  std::filesystem::remove_all(root, ec);
  return 0;
}
