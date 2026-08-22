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
  mq::client::ProduceResult result; assert(producer.produce("client", "key", "one", mq::client::AckMode::kOne, &result)); assert(result.offset == 0);
  std::vector<mq::client::ProducerMessage> batch{{"key", "two"}, {"key", "three"}}; std::vector<mq::client::ProduceResult> results;
  assert(producer.produceBatch("client", batch, mq::client::AckMode::kOne, &results)); assert(results.size() == 2 && results[0].offset == 1 && results[1].offset == 2);
  mq::client::MqConsumer consumer; assert(consumer.connect("localhost", server.port())); assert(consumer.subscribe("client", "group"));
  auto first = consumer.poll(); assert(first.has_value() && first->value == "one"); assert(consumer.commit(1));
  producer.close(); consumer.close(); server.Stop(); broker.Flush(); std::filesystem::remove_all(root, ec); return 0;
}
