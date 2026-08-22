#include <cassert>
#include <filesystem>
#include <string>
#include <vector>

#include "mq/core/storage_engine.h"
#include "mq/protocol/protocol_codec.h"

namespace {

void ProtocolRoundTrip() {
  mq::protocol::Request original;
  original.command = mq::protocol::Command::kProduce;
  original.request_id = 42;
  original.topic = "orders";
  original.payload = "key=value";
  std::string frame, error;
  assert(mq::protocol::ProtocolCodec::EncodeRequest(original, &frame, &error));
  mq::protocol::Request decoded;
  assert(mq::protocol::ProtocolCodec::DecodeRequest(frame, &decoded, &error));
  assert(decoded.request_id == original.request_id && decoded.topic == original.topic &&
         decoded.payload == original.payload);
  frame.pop_back();
  assert(!mq::protocol::ProtocolCodec::DecodeRequest(frame, &decoded, &error));
}

void RequestStreamDecoding() {
  mq::protocol::Request first;
  first.command = mq::protocol::Command::kHeartbeat;
  first.request_id = 1;
  first.topic = "orders";
  mq::protocol::Request second;
  second.command = mq::protocol::Command::kProduce;
  second.request_id = 2;
  second.topic = "orders";
  second.payload = "payload";
  std::string first_frame, second_frame, error;
  assert(mq::protocol::ProtocolCodec::EncodeRequest(first, &first_frame, &error));
  assert(mq::protocol::ProtocolCodec::EncodeRequest(second, &second_frame, &error));
  mq::protocol::RequestStreamDecoder decoder;
  std::vector<mq::protocol::Request> requests;
  assert(decoder.Push(std::string_view(first_frame).substr(0, 9), &requests, &error));
  assert(requests.empty());
  const std::string remaining = first_frame.substr(9) + second_frame;
  assert(decoder.Push(remaining, &requests, &error));
  assert(requests.size() == 2);
  assert(requests[0].request_id == first.request_id);
  assert(requests[1].request_id == second.request_id && requests[1].payload == second.payload);
}

void StorageRoundTrip() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_storage_test";
  std::error_code ec;
  std::filesystem::remove_all(root, ec);
  {
    mq::core::StorageEngine storage(root);
    assert(storage.Open());
    mq::core::Message message;
    assert(storage.Append("orders", 0, "id", "one", &message));
    assert(message.offset == 0);
    assert(storage.Append("orders", 0, "id", "two", &message));
    assert(message.offset == 1);
    assert(storage.Flush());
  }
  {
    mq::core::StorageEngine storage(root);
    assert(storage.Open());
    std::vector<mq::core::Message> messages;
    assert(storage.Read("orders", 0, 1, 1024, &messages));
    assert(messages.size() == 1 && messages.front().value == "two");
  }
  std::filesystem::remove_all(root, ec);
}

void SegmentsIndexesAndRetention() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_segment_test";
  std::error_code ec;
  std::filesystem::remove_all(root, ec);
  mq::core::StorageConfig config;
  config.segment_size_bytes = 128;
  config.index_interval = 2;
  config.retention_bytes = 1;
  config.cleaner_interval_ms = 60000;
  {
    mq::core::StorageEngine storage(root, config);
    assert(storage.Open());
    mq::core::Message message;
    for (int index = 0; index < 12; ++index) {
      assert(storage.Append("orders", 0, "k", std::string(40, static_cast<char>('a' + index)), &message));
    }
    assert(std::filesystem::exists(root / "queues" / "6f7264657273" / "0" /
                                    "00000000000000000001.log"));
    assert(std::filesystem::exists(root / "queues" / "6f7264657273" / "0" /
                                    "00000000000000000000.index"));
    std::vector<mq::core::Message> messages;
    assert(storage.Read("orders", 0, 8, 1000, &messages));
    assert(messages.size() == 4 && messages.front().offset == 8 && messages.back().offset == 11);
    assert(storage.CleanupExpiredSegments());
    std::size_t log_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(root / "queues" / "6f7264657273" / "0"))
      if (entry.path().extension() == ".log") ++log_count;
    assert(log_count == 1);
  }
  std::filesystem::remove_all(root, ec);
}

}  // namespace

int main() {
  ProtocolRoundTrip();
  RequestStreamDecoding();
  StorageRoundTrip();
  SegmentsIndexesAndRetention();
  return 0;
}
