#pragma once

#include <filesystem>
#include <chrono>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <thread>

#include "mq/core/queue_manager.h"
#include "mq/core/consumer_offset_store.h"
#include "mq/core/storage_engine.h"
#include "mq/core/topic_metadata_store.h"
#include "mq/protocol/commands.h"

namespace mq::server {

struct ReplicationPeer {
  std::string node_id;
  std::string host;
  std::uint16_t port = 0;
  bool leader = false;
};

class Broker {
 public:
  explicit Broker(std::filesystem::path data_dir);
  Broker(std::filesystem::path data_dir, core::StorageConfig storage_config);
  ~Broker();

  bool Open(std::string* error = nullptr);
  void ConfigureReplication(std::string node_id, std::vector<ReplicationPeer> peers,
                            std::size_t quorum = 0, bool follower = false);
  void ConfigureRateLimit(std::uint64_t produce_requests_per_second);
  void StartReplication();
  void StopReplication();
  bool Flush(std::string* error = nullptr);
  protocol::Response Handle(const protocol::Request& request);

 private:
  protocol::Response HandleCreateTopic(const protocol::Request& request);
  protocol::Response HandleListTopic(const protocol::Request& request);
  protocol::Response HandleMetrics(const protocol::Request& request);
  protocol::Response HandleDeleteTopic(const protocol::Request& request);
  protocol::Response HandleProduce(const protocol::Request& request, bool enforce_rate_limit = true);
  protocol::Response HandleProduceBatch(const protocol::Request& request);
  protocol::Response HandleFetch(const protocol::Request& request);
  protocol::Response HandleCommitOffset(const protocol::Request& request);
  protocol::Response HandleHeartbeat(const protocol::Request& request);
  protocol::Response HandleReplicaFetch(const protocol::Request& request);
  protocol::Response HandleReplicaAppend(const protocol::Request& request);
  protocol::Response HandleReplicaVote(const protocol::Request& request);
  protocol::Response MakeResponse(const protocol::Request& request, protocol::Status status,
                                  std::string payload = {}) const;
  bool Replicate(const std::string& topic, std::uint32_t partition, const core::Message& message);
  bool AllowProduceRequest();

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
  std::string node_id_ = "node-local";
  std::vector<ReplicationPeer> replication_peers_;
  std::size_t replication_quorum_ = 0;
  std::unique_ptr<class ReplicationCoordinator> replication_coordinator_;
  bool follower_ = false;
  std::atomic<bool> stop_replication_{false};
  std::condition_variable replication_cv_;
  std::mutex replication_mutex_;
  std::thread replication_thread_;
  std::unordered_map<std::string, std::uint64_t> replication_offsets_;
  std::atomic<std::uint64_t> request_count_{0};
  std::atomic<std::uint64_t> produce_count_{0};
  std::atomic<std::uint64_t> fetch_count_{0};
  std::atomic<std::uint64_t> error_count_{0};
  std::mutex rate_limit_mutex_;
  std::uint64_t produce_rate_limit_ = 0;
  std::uint64_t produce_window_count_ = 0;
  std::chrono::steady_clock::time_point produce_window_start_ = std::chrono::steady_clock::now();
  void ReplicationLoop();
};

}  // namespace mq::server
