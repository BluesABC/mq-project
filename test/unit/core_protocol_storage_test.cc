#include <cassert>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "mq/core/consumer_offset_store.h"
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
      assert(storage.Append("orders", 0, "k", std::string(40, static_cast<char>('a' + index)),
                            &message));
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
    for (const auto& entry :
         std::filesystem::directory_iterator(root / "queues" / "6f7264657273" / "0"))
      if (entry.path().extension() == ".log") ++log_count;
    assert(log_count == 1);
  }
  std::filesystem::remove_all(root, ec);
}

void RecoveryDropsCorruptAndTruncatedTail() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_recovery_test";
  std::error_code ec;
  std::filesystem::remove_all(root, ec);
  const auto log = root / "queues" / "6f7264657273" / "0" / "00000000000000000000.log";
  {
    mq::core::StorageEngine storage(root);
    assert(storage.Open());
    mq::core::Message message;
    assert(storage.Append("orders", 0, "k", "one", &message));
    assert(storage.Append("orders", 0, "k", "two", &message));
    assert(storage.Flush());
  }
  {
    std::fstream file(log, std::ios::in | std::ios::out | std::ios::binary);
    assert(file);
    file.seekg(-1, std::ios::end);
    char byte = 0;
    file.get(byte);
    file.seekp(-1, std::ios::end);
    file.put(static_cast<char>(byte ^ 0x7F));
  }
  {
    mq::core::StorageEngine storage(root);
    assert(storage.Open());
    std::vector<mq::core::Message> messages;
    assert(storage.Read("orders", 0, 0, 1024, &messages));
    assert(messages.size() == 1 && messages.front().value == "one");
  }

  const auto truncated_root = std::filesystem::temp_directory_path() / "mq_project_truncated_test";
  std::filesystem::remove_all(truncated_root, ec);
  const auto truncated_log =
      truncated_root / "queues" / "6f7264657273" / "0" / "00000000000000000000.log";
  {
    mq::core::StorageEngine storage(truncated_root);
    assert(storage.Open());
    mq::core::Message message;
    assert(storage.Append("orders", 0, "k", "one", &message));
    assert(storage.Append("orders", 0, "k", "two", &message));
    assert(storage.Flush());
  }
  assert(std::filesystem::file_size(truncated_log) > 3);
  std::filesystem::resize_file(truncated_log, std::filesystem::file_size(truncated_log) - 3);
  {
    mq::core::StorageEngine storage(truncated_root);
    assert(storage.Open());
    std::vector<mq::core::Message> messages;
    assert(storage.Read("orders", 0, 0, 1024, &messages));
    assert(messages.size() == 1 && messages.front().value == "one");
  }
  std::filesystem::remove_all(root, ec);
  std::filesystem::remove_all(truncated_root, ec);
}

void ConsumerOffsetsPersistAtomicallyAndIndependently() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_offset_store_test";
  std::error_code ec;
  std::filesystem::remove_all(root, ec);
  mq::core::ConsumerOffsetStore store(root / "metadata" / "consumer_offsets.meta");
  assert(store.Save({{"group-a", "orders", 0, 12}, {"group-b", "orders", 0, 4}}));
  std::vector<mq::core::ConsumerOffset> offsets;
  assert(store.Load(&offsets));
  assert(offsets.size() == 2);
  assert(offsets[0].group == "group-a" && offsets[0].offset == 12);
  assert(offsets[1].group == "group-b" && offsets[1].offset == 4);
  assert(std::filesystem::exists(root / "metadata" / "consumer_offsets.meta"));
  assert(!std::filesystem::exists(root / "metadata" / "consumer_offsets.meta.tmp"));
  std::filesystem::remove_all(root, ec);
}

}  // namespace

int main() {
  ProtocolRoundTrip();
  RequestStreamDecoding();
  StorageRoundTrip();
  SegmentsIndexesAndRetention();
  RecoveryDropsCorruptAndTruncatedTail();
  ConsumerOffsetsPersistAtomicallyAndIndependently();
  return 0;
}
