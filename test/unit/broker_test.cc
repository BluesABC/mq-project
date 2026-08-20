#include <cassert>
#include <filesystem>
#include <string>

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

std::uint16_t Get16(const std::string& input, std::size_t position) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(input[position])) << 8) |
         static_cast<unsigned char>(input[position + 1]);
}

std::uint32_t Get32(const std::string& input, std::size_t position) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}

std::uint64_t Get64(const std::string& input, std::size_t position) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}

std::vector<mq::core::TopicMetadata> ListTopics(mq::server::Broker* broker) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kListTopic;
  const auto response = broker->Handle(request);
  assert(response.status == mq::protocol::Status::kOk);
  assert(response.payload.size() >= 4);

  std::vector<mq::core::TopicMetadata> topics;
  std::size_t position = 4;
  const auto count = Get32(response.payload, 0);
  topics.reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    assert(position + 2 <= response.payload.size());
    const auto name_length = Get16(response.payload, position);
    position += 2;
    assert(position + name_length + 4 <= response.payload.size());
    mq::core::TopicMetadata topic;
    topic.name = response.payload.substr(position, name_length);
    position += name_length;
    topic.partition_count = Get32(response.payload, position);
    position += 4;
    topics.push_back(std::move(topic));
  }
  assert(position == response.payload.size());
  return topics;
}

mq::protocol::Request CreateTopicRequest(std::uint64_t request_id) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kCreateTopic;
  request.request_id = request_id;
  request.topic = "orders";
  Put32(&request.payload, 3);
  return request;
}

mq::protocol::Request CreateTopicRequest(std::uint64_t request_id, std::string topic,
                                         std::uint32_t partitions) {
  auto request = CreateTopicRequest(request_id);
  request.topic = std::move(topic);
  request.payload.clear();
  Put32(&request.payload, partitions);
  return request;
}

mq::protocol::Request ProduceRequest(std::uint64_t request_id, std::string key, std::string value) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kProduce;
  request.request_id = request_id;
  request.topic = "orders";
  Put32(&request.payload, mq::core::QueueManager::kAutoPartition);
  Put16(&request.payload, static_cast<std::uint16_t>(key.size()));
  request.payload.append(key);
  Put32(&request.payload, static_cast<std::uint32_t>(value.size()));
  request.payload.append(value);
  return request;
}

mq::protocol::Request FetchRequest(std::uint64_t request_id, std::uint32_t partition) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kFetch;
  request.request_id = request_id;
  request.topic = "orders";
  Put32(&request.payload, partition);
  Put64(&request.payload, 0);
  Put32(&request.payload, 1024);
  return request;
}

void CreateProduceFetch() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_broker_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  mq::server::Broker broker(root);
  assert(broker.Open());
  const auto created = broker.Handle(CreateTopicRequest(1));
  assert(created.status == mq::protocol::Status::kOk && created.request_id == 1);
  const auto duplicate = broker.Handle(CreateTopicRequest(2));
  assert(duplicate.status == mq::protocol::Status::kTopicExists);
  mq::protocol::Request list_request;
  list_request.command = mq::protocol::Command::kListTopic;
  list_request.request_id = 3;
  const auto listed = broker.Handle(list_request);
  assert(listed.status == mq::protocol::Status::kOk && Get32(listed.payload, 0) == 1);
  assert(Get32(listed.payload, listed.payload.size() - 4) == 3);
  const auto produced = broker.Handle(ProduceRequest(3, "customer-42", "created"));
  assert(produced.status == mq::protocol::Status::kOk && produced.payload.size() == 12);
  const std::uint32_t partition = Get32(produced.payload, 0);
  assert(partition < 3 && Get64(produced.payload, 4) == 0);
  const auto fetched = broker.Handle(FetchRequest(4, partition));
  assert(fetched.status == mq::protocol::Status::kOk);
  assert(Get32(fetched.payload, 0) == 1);
  assert(Get64(fetched.payload, 4) == 0);
  std::filesystem::remove_all(root, error);
}

void RestoresTopicMetadata() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_metadata_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  {
    mq::server::Broker broker(root);
    assert(broker.Open());
    assert(broker.Handle(CreateTopicRequest(1, "alpha", 1)).status == mq::protocol::Status::kOk);
    assert(broker.Handle(CreateTopicRequest(2, "beta", 3)).status == mq::protocol::Status::kOk);
    assert(broker.Handle(CreateTopicRequest(3, "gamma", 8)).status == mq::protocol::Status::kOk);
    assert(std::filesystem::exists(root / "metadata" / "topics.meta"));
  }
  {
    mq::server::Broker broker(root);
    assert(broker.Open());
    const auto topics = ListTopics(&broker);
    assert(topics.size() == 3);
    assert(topics[0].name == "alpha" && topics[0].partition_count == 1);
    assert(topics[1].name == "beta" && topics[1].partition_count == 3);
    assert(topics[2].name == "gamma" && topics[2].partition_count == 8);
  }
  std::filesystem::remove_all(root, error);
}

}  // namespace

int main() {
  CreateProduceFetch();
  RestoresTopicMetadata();
  return 0;
}
