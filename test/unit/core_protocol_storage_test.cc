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

}  // namespace

int main() {
  ProtocolRoundTrip();
  StorageRoundTrip();
  return 0;
}
