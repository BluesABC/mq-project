#pragma once

#include <filesystem>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "mq/core/queue_manager.h"
#include "mq/core/consumer_offset_store.h"
#include "mq/core/storage_engine.h"
#include "mq/core/topic_metadata_store.h"
#include "mq/protocol/commands.h"

namespace mq::server {

class Broker {
 public:
  explicit Broker(std::filesystem::path data_dir);
  Broker(std::filesystem::path data_dir, core::StorageConfig storage_config);

  bool Open(std::string* error = nullptr);
  bool Flush(std::string* error = nullptr);
  protocol::Response Handle(const protocol::Request& request);

 private:
  protocol::Response HandleCreateTopic(const protocol::Request& request);
  protocol::Response HandleListTopic(const protocol::Request& request);
  protocol::Response HandleDeleteTopic(const protocol::Request& request);
  protocol::Response HandleProduce(const protocol::Request& request);
  protocol::Response HandleProduceBatch(const protocol::Request& request);
  protocol::Response HandleFetch(const protocol::Request& request);
  protocol::Response HandleCommitOffset(const protocol::Request& request);
  protocol::Response HandleHeartbeat(const protocol::Request& request);
  protocol::Response HandleReplicaFetch(const protocol::Request& request);
  protocol::Response HandleReplicaAppend(const protocol::Request& request);
  protocol::Response MakeResponse(const protocol::Request& request, protocol::Status status,
                                  std::string payload = {}) const;

  core::StorageEngine storage_;
  core::TopicMetadataStore metadata_store_;
  core::ConsumerOffsetStore offset_store_;
  core::StorageConfig storage_config_;
  std::vector<core::ConsumerOffset> consumer_offsets_;
  core::QueueManager queues_;
  std::mutex topic_metadata_mutex_;
  bool opened_ = false;
  std::mutex idempotency_mutex_;
  std::unordered_map<std::string, protocol::Response> idempotency_cache_;
};

}  // namespace mq::server
