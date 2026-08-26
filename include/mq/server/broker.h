#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "mq/core/consumer_offset_store.h"
#include "mq/core/queue_manager.h"
#include "mq/core/storage_engine.h"
#include "mq/core/topic_metadata_store.h"
#include "mq/protocol/commands.h"

namespace mq::server {

// Broker 编排协议、路由、存储、位点和复制模块，是请求进入业务层的唯一入口。
struct ReplicationPeer {
  std::string node_id;
  std::string host;
  std::uint16_t port = 0;
  bool leader = false;
};

// 同一客户端 Token 共享一套 ACL；空的 Topic 列表表示不限制 Topic。
struct ClientAuthorization {
  bool allow_admin = true;
  bool allow_produce = true;
  bool allow_consume = true;
  std::vector<std::string> produce_topics;
  std::vector<std::string> consume_topics;
};

class Broker {
 public:
  explicit Broker(std::filesystem::path data_dir);
  Broker(std::filesystem::path data_dir, core::StorageConfig storage_config);
  ~Broker();

  bool Open(std::string* error = nullptr);
  void ConfigureReplication(std::string node_id, std::vector<ReplicationPeer> peers,
                            std::size_t quorum = 0, bool follower = false,
                            std::string auth_token = {});
  void ConfigureClientAuth(std::string auth_token, ClientAuthorization authorization = {});
  void ConfigureRateLimit(std::uint64_t produce_requests_per_second);
  void ConfigureTopicQuota(std::uint64_t produce_bytes_per_second);
  void StartReplication();
  void StopReplication();
  bool Flush(std::string* error = nullptr);
  // Handle 保持协议 request_id 原样返回，便于客户端匹配响应和处理重试。
  protocol::Response Handle(const protocol::Request& request);

 private:
  protocol::Response HandleCreateTopic(const protocol::Request& request);
  protocol::Response HandleListTopic(const protocol::Request& request);
  protocol::Response HandleMetrics(const protocol::Request& request);
  protocol::Response HandleDeleteTopic(const protocol::Request& request);
  protocol::Response HandleProduce(const protocol::Request& request, bool enforce_rate_limit = true,
                                   bool enforce_topic_quota = true);
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
  bool AllowTopicBytes(const std::string& topic, std::uint64_t bytes);
  bool ValidateReplicationRequest(const protocol::Request& request,
                                  protocol::Request* normalized) const;
  bool ValidateClientAuth(const protocol::Request& request, protocol::Request* normalized) const;
  bool AuthorizeClientRequest(const protocol::Request& request) const;
  static bool TopicAllowed(const std::vector<std::string>& topics, const std::string& topic);
  static std::string PartitionKey(const std::string& topic, std::uint32_t partition);

  core::StorageEngine storage_;
  core::TopicMetadataStore metadata_store_;
  core::ConsumerOffsetStore offset_store_;
  core::StorageConfig storage_config_;
  std::vector<core::ConsumerOffset> consumer_offsets_;
  core::QueueManager queues_;
  std::mutex topic_metadata_mutex_;
  bool opened_ = false;
  std::mutex idempotency_mutex_;
  struct IdempotencyEntry {
    protocol::Response response;
    std::chrono::steady_clock::time_point expires_at;
  };
  std::unordered_map<std::string, IdempotencyEntry> idempotency_cache_;
  std::string node_id_ = "node-local";
  std::vector<ReplicationPeer> replication_peers_;
  std::string replication_auth_token_;
  std::string client_auth_token_;
  ClientAuthorization client_authorization_;
  bool replication_configured_ = false;
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
  long double produce_tokens_ = 0;
  std::chrono::steady_clock::time_point produce_last_refill_ = std::chrono::steady_clock::now();
  struct TopicQuotaWindow {
    std::uint64_t bytes = 0;
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  };
  std::mutex topic_quota_mutex_;
  std::uint64_t topic_produce_quota_ = 0;
  std::unordered_map<std::string, TopicQuotaWindow> topic_quota_windows_;
  void ReplicationLoop();
};

}  // namespace mq::server
