#include "mq/server/replication_client.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <thread>

#include "mq/network/tcp_server.h"
#include "mq/server/broker.h"

namespace {
void Put16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}
void Put32(std::string* out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}
void Put64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}
std::uint32_t Get32(const std::string& value, std::size_t position) {
  std::uint32_t result = 0;
  for (int i = 0; i < 4; ++i)
    result = (result << 8) | static_cast<unsigned char>(value[position + i]);
  return result;
}

mq::protocol::Request CreateTopic() {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kCreateTopic;
  request.topic = "replication";
  Put32(&request.payload, 1);
  return request;
}
mq::protocol::Request Produce() {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kProduce;
  request.topic = "replication";
  Put32(&request.payload, 0xFFFFFFFFu);
  Put16(&request.payload, 1);
  request.payload.push_back('k');
  Put32(&request.payload, 1);
  request.payload.push_back('v');
  return request;
}
mq::protocol::Request ProduceAckAll() {
  auto request = Produce();
  request.flags = mq::protocol::kAckAll;
  return request;
}
}  // namespace

int main() {
  const auto root = std::filesystem::temp_directory_path() / "mq_replication_client_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  mq::server::Broker broker(root);
  assert(broker.Open());
  assert(broker.Handle(CreateTopic()).status == mq::protocol::Status::kOk);
  const auto produced = broker.Handle(Produce());
  assert(produced.status == mq::protocol::Status::kOk);
  mq::network::TcpServer server("127.0.0.1", 0, 2,
                                [&broker](const auto& request) { return broker.Handle(request); });
  assert(server.Start());
  mq::server::ReplicationClient client("127.0.0.1", server.port());
  std::vector<mq::core::Message> messages;
  assert(client.Fetch("replication", 0, 0, 1024, &messages));
  assert(messages.size() == 1 && messages[0].value == "v");
  assert(client.Heartbeat("follower-a", 1));
  server.Stop();
  assert(!client.Heartbeat("follower-a", 1));
  const auto follower_root = root / "follower";
  const auto leader_root = root / "leader";
  mq::server::Broker follower(follower_root);
  assert(follower.Open());
  assert(follower.Handle(CreateTopic()).status == mq::protocol::Status::kOk);
  mq::network::TcpServer follower_server(
      "127.0.0.1", 0, 2, [&follower](const auto& request) { return follower.Handle(request); });
  assert(follower_server.Start());
  mq::server::Broker leader(leader_root);
  assert(leader.Open());
  assert(leader.Handle(CreateTopic()).status == mq::protocol::Status::kOk);
  leader.ConfigureReplication("leader", {{"follower", "127.0.0.1", follower_server.port()}}, 2);
  assert(leader.Handle(ProduceAckAll()).status == mq::protocol::Status::kOk);
  follower_server.Stop();
  assert(leader.Handle(ProduceAckAll()).status == mq::protocol::Status::kStorageError);
  std::filesystem::remove_all(follower_root, error);
  std::filesystem::remove_all(leader_root, error);
  std::filesystem::remove_all(root, error);
  return 0;
}
