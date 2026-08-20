#pragma once

#include <filesystem>
#include <mutex>
#include <string>

#include "mq/core/queue_manager.h"
#include "mq/core/storage_engine.h"
#include "mq/core/topic_metadata_store.h"
#include "mq/protocol/commands.h"

namespace mq::server {

class Broker {
 public:
  explicit Broker(std::filesystem::path data_dir);

  bool Open(std::string* error = nullptr);
  protocol::Response Handle(const protocol::Request& request);

 private:
  protocol::Response HandleCreateTopic(const protocol::Request& request);
  protocol::Response HandleListTopic(const protocol::Request& request);
  protocol::Response HandleDeleteTopic(const protocol::Request& request);
  protocol::Response HandleProduce(const protocol::Request& request);
  protocol::Response HandleFetch(const protocol::Request& request);
  protocol::Response MakeResponse(const protocol::Request& request, protocol::Status status,
                                  std::string payload = {}) const;

  core::StorageEngine storage_;
  core::TopicMetadataStore metadata_store_;
  core::QueueManager queues_;
  std::mutex topic_metadata_mutex_;
  bool opened_ = false;
};

}  // namespace mq::server
