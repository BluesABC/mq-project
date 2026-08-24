#include "mq/server/broker.h"

#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>
#include <utility>
#include <vector>
#include <sstream>
#include <limits>

#include "mq/core/logger.h"
#include "mq/server/replication.h"
#include "mq/server/replication_client.h"

namespace mq::server {
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

bool Take(std::string_view input, std::size_t* position, std::size_t count) {
  if (position == nullptr || *position > input.size() || count > input.size() - *position) return false;
  *position += count;
  return true;
}

std::uint16_t Get16(std::string_view input, std::size_t position) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(input[position])) << 8) |
         static_cast<unsigned char>(input[position + 1]);
}

std::uint32_t Get32(std::string_view input, std::size_t position) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}

std::uint64_t Get64(std::string_view input, std::size_t position) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}
std::string IdempotencyKey(std::uint64_t producer_id, std::uint64_t sequence) {
  return std::to_string(producer_id) + ":" + std::to_string(sequence);
}

}  // namespace

Broker::Broker(std::filesystem::path data_dir) : Broker(std::move(data_dir), core::StorageConfig{}) {}
Broker::Broker(std::filesystem::path data_dir, core::StorageConfig storage_config)
    : storage_(data_dir, storage_config), metadata_store_(data_dir / "metadata" / "topics.meta"),
      offset_store_(data_dir / "metadata" / "consumer_offsets.meta"), storage_config_(storage_config),
      replication_coordinator_(std::make_unique<ReplicationCoordinator>(node_id_)) {}

Broker::~Broker() { StopReplication(); }

void Broker::ConfigureReplication(std::string node_id, std::vector<ReplicationPeer> peers, std::size_t quorum, bool follower) {
  node_id_ = std::move(node_id);
  replication_peers_ = std::move(peers);
  replication_quorum_ = quorum == 0 ? replication_peers_.size() + 1 : quorum;
  follower_ = follower;
  replication_coordinator_ = std::make_unique<ReplicationCoordinator>(node_id_, follower ? ReplicaRole::kFollower : ReplicaRole::kLeader);
  for (const auto& peer : replication_peers_) replication_coordinator_->RegisterReplica(peer.node_id);
}

void Broker::ConfigureRateLimit(std::uint64_t produce_requests_per_second) {
  std::lock_guard lock(rate_limit_mutex_);
  produce_rate_limit_ = produce_requests_per_second;
  produce_window_count_ = 0;
  produce_window_start_ = std::chrono::steady_clock::now();
}

void Broker::ConfigureTopicQuota(std::uint64_t produce_bytes_per_second) {
  std::lock_guard lock(topic_quota_mutex_);
  topic_produce_quota_ = produce_bytes_per_second;
  topic_quota_windows_.clear();
}

bool Broker::AllowProduceRequest() {
  std::lock_guard lock(rate_limit_mutex_);
  if (produce_rate_limit_ == 0) return true;
  const auto now = std::chrono::steady_clock::now();
  if (now - produce_window_start_ >= std::chrono::seconds(1)) {
    produce_window_start_ = now;
    produce_window_count_ = 0;
  }
  if (produce_window_count_ >= produce_rate_limit_) return false;
  ++produce_window_count_;
  return true;
}

bool Broker::AllowTopicBytes(const std::string& topic, std::uint64_t bytes) {
  std::lock_guard lock(topic_quota_mutex_);
  if (topic_produce_quota_ == 0) return true;
  if (bytes > topic_produce_quota_) return false;
  auto& window = topic_quota_windows_[topic];
  const auto now = std::chrono::steady_clock::now();
  if (now - window.start >= std::chrono::seconds(1)) {
    window.start = now;
    window.bytes = 0;
  }
  if (window.bytes > topic_produce_quota_ - bytes) return false;
  window.bytes += bytes;
  return true;
}

void Broker::StartReplication() {
  if (replication_thread_.joinable()) return;
  stop_replication_.store(false, std::memory_order_release);
  replication_thread_ = std::thread(&Broker::ReplicationLoop, this);
}

void Broker::StopReplication() {
  stop_replication_.store(true, std::memory_order_release);
  replication_cv_.notify_all();
  if (replication_thread_.joinable()) replication_thread_.join();
}

void Broker::ReplicationLoop() {
  try {
    auto last_election = std::chrono::steady_clock::now() - std::chrono::seconds(2);
    while (!stop_replication_.load(std::memory_order_acquire)) {
      if (replication_coordinator_->role() != ReplicaRole::kLeader) {
        bool leader_seen = false;
        for (const auto& peer : replication_peers_) {
          if (replication_coordinator_->role() == ReplicaRole::kFollower && peer.leader) {
            ReplicationClient client(peer.host, peer.port);
            if (client.Heartbeat(node_id_, 0)) leader_seen = true;
            for (const auto& topic : queues_.ListTopics()) {
              for (std::uint32_t partition = 0; partition < topic.partition_count; ++partition) {
              const auto key = topic.name + ":" + std::to_string(partition);
              std::uint64_t offset = 0;
              { std::lock_guard lock(replication_mutex_); offset = replication_offsets_[key]; }
              std::vector<core::Message> messages;
              if (!client.Fetch(topic.name, partition, offset, 1024 * 1024, &messages)) continue;
              leader_seen = true;
              bool applied = true;
              for (const auto& message : messages) {
                std::string error;
                if (!storage_.AppendReplica(topic.name, partition, message, &error)) { applied = false; break; }
              }
              if (applied && !messages.empty()) {
                offset = messages.back().offset + 1;
                { std::lock_guard lock(replication_mutex_); replication_offsets_[key] = offset; }
                client.Heartbeat(node_id_, offset);
              }
              }
            }
          }
        }
        if (!leader_seen && std::chrono::steady_clock::now() - last_election >= std::chrono::seconds(1)) {
          const auto election = replication_coordinator_->BeginElection();
          std::size_t votes = 1;
          for (const auto& peer : replication_peers_) {
            if (peer.leader) continue;
            ReplicationClient client(peer.host, peer.port);
            bool granted = false;
            if (client.Vote(election.term, node_id_, &granted) && granted) {
              ++votes;
              replication_coordinator_->ObserveVote(election.term, peer.node_id, true);
            }
          }
          if (votes >= (replication_peers_.size() + 2) / 2 + 1) replication_coordinator_->ObserveVote(election.term, node_id_, true);
          last_election = std::chrono::steady_clock::now();
        }
      } else {
        for (const auto& peer : replication_peers_) {
          ReplicationClient client(peer.host, peer.port);
          if (client.Heartbeat(node_id_, 0, replication_coordinator_->term(), replication_coordinator_->commitIndex())) replication_coordinator_->ObserveHeartbeat(peer.node_id, 0);
        }
      }
      std::unique_lock lock(replication_mutex_);
      replication_cv_.wait_for(lock, std::chrono::milliseconds(250), [this] {
        return stop_replication_.load(std::memory_order_acquire);
      });
    }
  } catch (...) {
    // Replication is best effort; a peer failure must not terminate Broker.
  }
}

bool Broker::Open(std::string* error) {
  if (opened_) return true;
  if (!storage_.Open(error)) {
    core::Logger::Instance().Log(core::LogLevel::kCritical, "broker storage initialization failed");
    return false;
  }
  std::vector<core::TopicMetadata> topics;
  if (!metadata_store_.Load(&topics, error) || !queues_.ReplaceTopics(std::move(topics), error)) return false;
  if (!offset_store_.Load(&consumer_offsets_, error)) return false;
  opened_ = true;
  core::Logger::Instance().Log(core::LogLevel::kInfo, "broker storage initialized");
  return true;
}

protocol::Response Broker::Handle(const protocol::Request& request) {
  request_count_.fetch_add(1, std::memory_order_relaxed);
  if (!opened_) {
    error_count_.fetch_add(1, std::memory_order_relaxed);
    core::Logger::Instance().Log(core::LogLevel::kError, "broker request received before open");
    return MakeResponse(request, protocol::Status::kInternalError);
  }
  if (request.command == protocol::Command::kProduce || request.command == protocol::Command::kProduceBatch) {
    produce_count_.fetch_add(1, std::memory_order_relaxed);
  } else if (request.command == protocol::Command::kFetch) {
    fetch_count_.fetch_add(1, std::memory_order_relaxed);
  }
  protocol::Response response;
  switch (request.command) {
    case protocol::Command::kCreateTopic:
      response = HandleCreateTopic(request); break;
    case protocol::Command::kListTopic:
      response = HandleListTopic(request); break;
    case protocol::Command::kMetrics:
      response = HandleMetrics(request); break;
    case protocol::Command::kDeleteTopic:
      response = HandleDeleteTopic(request); break;
    case protocol::Command::kProduce:
      response = HandleProduce(request); break;
    case protocol::Command::kProduceBatch:
      response = HandleProduceBatch(request); break;
    case protocol::Command::kFetch:
      response = HandleFetch(request); break;
    case protocol::Command::kCommitOffset:
      response = HandleCommitOffset(request); break;
    case protocol::Command::kHeartbeat:
      response = HandleHeartbeat(request); break;
    case protocol::Command::kReplicaFetch:
      response = HandleReplicaFetch(request); break;
    case protocol::Command::kReplicaAppend:
      response = HandleReplicaAppend(request); break;
    case protocol::Command::kReplicaVote:
      response = HandleReplicaVote(request); break;
    default:
      response = MakeResponse(request, protocol::Status::kBadRequest); break;
  }
  if (response.status != protocol::Status::kOk) error_count_.fetch_add(1, std::memory_order_relaxed);
  return response;
}

bool Broker::Flush(std::string* error) {
  return storage_.Flush(error);
}

protocol::Response Broker::HandleHeartbeat(const protocol::Request& request) {
  if ((request.flags & protocol::kFlagReplication) != 0) {
    if (request.topic.empty() || request.payload.size() < 10) return MakeResponse(request, protocol::Status::kBadRequest);
    const auto node_size = Get16(request.payload, 0);
    if (node_size == 0 || (2ULL + node_size + 8 != request.payload.size() && 2ULL + node_size + 24 != request.payload.size())) return MakeResponse(request, protocol::Status::kBadRequest);
    const std::string node_id = request.payload.substr(2, node_size);
    const auto replicated_offset = Get64(request.payload, 2 + node_size);
    std::string response_payload;
    Put64(&response_payload, replicated_offset);
    if (request.payload.size() == 2ULL + node_size + 24) {
      const auto term = Get64(request.payload, 10 + node_size);
      const auto commit = Get64(request.payload, 18 + node_size);
      if (!replication_coordinator_->ObserveAppend(term, node_id, replicated_offset, commit)) return MakeResponse(request, protocol::Status::kStorageError);
    } else {
      replication_coordinator_->ObserveHeartbeat(node_id, replicated_offset);
    }
    return MakeResponse(request, protocol::Status::kOk, std::move(response_payload));
  }
  if (!request.payload.empty()) return MakeResponse(request, protocol::Status::kBadRequest);
  std::string error;
  if (!storage_.Flush(&error)) return MakeResponse(request, protocol::Status::kStorageError);
  return MakeResponse(request, protocol::Status::kOk);
}

protocol::Response Broker::HandleReplicaFetch(const protocol::Request& request) {
  if ((request.flags & protocol::kFlagReplication) == 0 || request.payload.size() != 16)
    return MakeResponse(request, protocol::Status::kBadRequest);
  const auto partition = Get32(request.payload, 0);
  const auto offset = Get64(request.payload, 4);
  const auto max_bytes = Get32(request.payload, 12);
  if (max_bytes == 0) return MakeResponse(request, protocol::Status::kBadRequest);
  std::vector<core::Message> messages;
  std::string error;
  if (!storage_.Read(request.topic, partition, offset, max_bytes, &messages, &error))
    return MakeResponse(request, error == "unknown topic" ? protocol::Status::kUnknownTopic : protocol::Status::kStorageError);
  std::string payload;
  Put32(&payload, static_cast<std::uint32_t>(messages.size()));
  for (const auto& message : messages) {
    Put64(&payload, message.offset);
    Put64(&payload, static_cast<std::uint64_t>(message.timestamp_ms));
    Put16(&payload, static_cast<std::uint16_t>(message.key.size()));
    payload.append(message.key);
    Put32(&payload, static_cast<std::uint32_t>(message.value.size()));
    payload.append(message.value);
  }
  return MakeResponse(request, protocol::Status::kOk, std::move(payload));
}

protocol::Response Broker::HandleReplicaAppend(const protocol::Request& request) {
  if ((request.flags & protocol::kFlagReplication) == 0 || request.payload.size() < 8)
    return MakeResponse(request, protocol::Status::kBadRequest);
  std::size_t position = 0;
  if ((request.flags & protocol::kFlagReplicationTerm) != 0) {
    if (request.payload.size() < 26) return MakeResponse(request, protocol::Status::kBadRequest);
    const auto term = Get64(request.payload, 0);
    const auto commit = Get64(request.payload, 8);
    const auto leader_size = Get16(request.payload, 16);
    if (leader_size == 0 || request.payload.size() < 18ULL + leader_size + 8) return MakeResponse(request, protocol::Status::kBadRequest);
    if (!replication_coordinator_->ObserveAppend(term, request.payload.substr(18, leader_size), 0, commit)) return MakeResponse(request, protocol::Status::kStorageError);
    position = 18 + leader_size;
  }
  const auto partition = Get32(request.payload, position); position += 4;
  const auto count = Get32(request.payload, position); position += 4;
  if (count == 0 || count > 10000) return MakeResponse(request, protocol::Status::kBadRequest);
  std::string error;
  for (std::uint32_t index = 0; index < count; ++index) {
    if (position + 18 > request.payload.size()) return MakeResponse(request, protocol::Status::kBadRequest);
    core::Message message;
    message.offset = Get64(request.payload, position); position += 8;
    message.timestamp_ms = static_cast<std::int64_t>(Get64(request.payload, position)); position += 8;
    const auto key_size = Get16(request.payload, position); position += 2;
    if (position + key_size + 4 > request.payload.size()) return MakeResponse(request, protocol::Status::kBadRequest);
    message.key.assign(request.payload, position, key_size); position += key_size;
    const auto value_size = Get32(request.payload, position); position += 4;
    if (position + value_size > request.payload.size()) return MakeResponse(request, protocol::Status::kBadRequest);
    message.value.assign(request.payload, position, value_size); position += value_size;
    if (!storage_.AppendReplica(request.topic, partition, message, &error))
      return MakeResponse(request, error == "replica offset gap" ? protocol::Status::kInvalidOffset : protocol::Status::kStorageError);
  }
  if (position != request.payload.size()) return MakeResponse(request, protocol::Status::kBadRequest);
  return MakeResponse(request, protocol::Status::kOk);
}

protocol::Response Broker::HandleReplicaVote(const protocol::Request& request) {
  if ((request.flags & protocol::kFlagReplication) == 0 || request.payload.size() < 10)
    return MakeResponse(request, protocol::Status::kBadRequest);
  const auto term = Get64(request.payload, 0);
  const auto size = Get16(request.payload, 8);
  if (size == 0 || request.payload.size() != 10ULL + size)
    return MakeResponse(request, protocol::Status::kBadRequest);
  const bool granted = replication_coordinator_->RequestVote(term, request.payload.substr(10, size));
  std::string payload(1, granted ? '\1' : '\0');
  return MakeResponse(request, protocol::Status::kOk, std::move(payload));
}

protocol::Response Broker::HandleCommitOffset(const protocol::Request& request) {
  std::string_view payload = request.payload; std::size_t position = 0;
  if (!Take(payload, &position, 2)) return MakeResponse(request, protocol::Status::kBadRequest);
  const auto group_size = Get16(payload, 0); const std::size_t group_position = position;
  if (!Take(payload, &position, group_size) || !Take(payload, &position, 4) || !Take(payload, &position, 8) || position != payload.size()) return MakeResponse(request, protocol::Status::kBadRequest);
  const auto partition = Get32(payload, group_position + group_size); const auto offset = Get64(payload, group_position + group_size + 4);
  if (group_size == 0 || request.topic.empty()) return MakeResponse(request, protocol::Status::kBadRequest);
  std::uint32_t resolved_partition = 0; std::string route_error;
  if (!queues_.ResolvePartition(request.topic, partition, "", &resolved_partition, &route_error)) {
    return MakeResponse(request, route_error == "unknown topic" ? protocol::Status::kUnknownTopic : protocol::Status::kInvalidOffset);
  }
  std::lock_guard<std::mutex> lock(topic_metadata_mutex_);
  const std::string group(payload.substr(group_position, group_size));
  auto it = std::find_if(consumer_offsets_.begin(), consumer_offsets_.end(), [&](const core::ConsumerOffset& item) { return item.group == group && item.topic == request.topic && item.partition == partition; });
  if (it == consumer_offsets_.end()) consumer_offsets_.push_back({group, request.topic, partition, offset}); else it->offset = offset;
  std::string error;
  if (!offset_store_.Save(consumer_offsets_, &error)) return MakeResponse(request, protocol::Status::kStorageError);
  return MakeResponse(request, protocol::Status::kOk);
}

protocol::Response Broker::HandleCreateTopic(const protocol::Request& request) {
  if (request.payload.size() != 4) return MakeResponse(request, protocol::Status::kBadRequest);
  std::lock_guard<std::mutex> lock(topic_metadata_mutex_);
  std::string error;
  if (queues_.CreateTopic(request.topic, Get32(request.payload, 0), &error)) {
    if (!metadata_store_.Save(queues_.ListTopics(), &error)) {
      queues_.DeleteTopic(request.topic, nullptr);
      return MakeResponse(request, protocol::Status::kStorageError);
    }
    return MakeResponse(request, protocol::Status::kOk);
  }
  return MakeResponse(request, error == "topic already exists" ? protocol::Status::kTopicExists
                                                                : protocol::Status::kBadRequest);
}

protocol::Response Broker::HandleDeleteTopic(const protocol::Request& request) {
  if (!request.payload.empty()) return MakeResponse(request, protocol::Status::kBadRequest);
  std::lock_guard<std::mutex> lock(topic_metadata_mutex_);
  std::string error;
  core::TopicMetadata deleted_topic;
  if (!queues_.GetTopic(request.topic, &deleted_topic)) {
    return MakeResponse(request, protocol::Status::kUnknownTopic);
  }
  if (!queues_.DeleteTopic(request.topic, &error)) return MakeResponse(request, protocol::Status::kUnknownTopic);
  if (!metadata_store_.Save(queues_.ListTopics(), &error)) {
    queues_.CreateTopic(std::move(deleted_topic.name), deleted_topic.partition_count, nullptr);
    return MakeResponse(request, protocol::Status::kStorageError);
  }
  return MakeResponse(request, protocol::Status::kOk);
}

protocol::Response Broker::HandleListTopic(const protocol::Request& request) {
  if (!request.topic.empty() || !request.payload.empty()) {
    return MakeResponse(request, protocol::Status::kBadRequest);
  }
  const auto topics = queues_.ListTopics();
  std::string payload;
  Put32(&payload, static_cast<std::uint32_t>(topics.size()));
  for (const auto& topic : topics) {
    Put16(&payload, static_cast<std::uint16_t>(topic.name.size()));
    payload.append(topic.name);
    Put32(&payload, topic.partition_count);
  }
  return MakeResponse(request, protocol::Status::kOk, std::move(payload));
}

protocol::Response Broker::HandleMetrics(const protocol::Request& request) {
  if (!request.topic.empty() || !request.payload.empty() || (request.flags & protocol::kFlagReplication) != 0)
    return MakeResponse(request, protocol::Status::kBadRequest);
  std::string role = "unknown";
  if (replication_coordinator_) {
    switch (replication_coordinator_->role()) {
      case ReplicaRole::kLeader: role = "leader"; break;
      case ReplicaRole::kFollower: role = "follower"; break;
      case ReplicaRole::kCandidate: role = "candidate"; break;
    }
  }
  std::uint64_t topic_quota = 0;
  std::uint64_t topic_quota_used = 0;
  {
    std::lock_guard lock(topic_quota_mutex_);
    topic_quota = topic_produce_quota_;
    for (const auto& entry : topic_quota_windows_) topic_quota_used += entry.second.bytes;
  }
  std::ostringstream metrics;
  metrics << "# TYPE mq_requests_total counter\n"
          << "mq_requests_total " << request_count_.load(std::memory_order_relaxed) << "\n"
          << "# TYPE mq_produce_total counter\n"
          << "mq_produce_total " << produce_count_.load(std::memory_order_relaxed) << "\n"
          << "# TYPE mq_fetch_total counter\n"
          << "mq_fetch_total " << fetch_count_.load(std::memory_order_relaxed) << "\n"
          << "# TYPE mq_errors_total counter\n"
          << "mq_errors_total " << error_count_.load(std::memory_order_relaxed) << "\n"
          << "# TYPE mq_replication_term gauge\n"
          << "mq_replication_term " << (replication_coordinator_ ? replication_coordinator_->term() : 0) << "\n"
          << "# TYPE mq_commit_index gauge\n"
          << "mq_commit_index " << (replication_coordinator_ ? replication_coordinator_->commitIndex() : 0) << "\n"
          << "# TYPE mq_topic_produce_quota_bytes_per_second gauge\n"
          << "mq_topic_produce_quota_bytes_per_second " << topic_quota << "\n"
          << "# TYPE mq_topic_produce_bytes_used gauge\n"
          << "mq_topic_produce_bytes_used " << topic_quota_used << "\n"
          << "mq_replication_role{role=\"" << role << "\"} 1\n";
  return MakeResponse(request, protocol::Status::kOk, metrics.str());
}

protocol::Response Broker::HandleProduce(const protocol::Request& request, bool enforce_rate_limit,
                                         bool enforce_topic_quota) {
  if ((request.flags & protocol::kFlagReplication) == 0 &&
      replication_coordinator_->role() != ReplicaRole::kLeader) {
    return MakeResponse(request, protocol::Status::kNotLeader);
  }
  if (enforce_rate_limit && (request.flags & protocol::kFlagReplication) == 0 && !AllowProduceRequest())
    return MakeResponse(request, protocol::Status::kRateLimited);
  std::string_view payload = request.payload;
  std::size_t position = 0;
  std::uint64_t producer_id = 0, sequence = 0;
  const bool has_metadata = (request.flags & protocol::kFlagProducerMetadata) != 0;
  if (has_metadata) {
    if (!Take(payload, &position, 16)) return MakeResponse(request, protocol::Status::kBadRequest);
    producer_id = Get64(payload, 0); sequence = Get64(payload, 8);
    std::lock_guard<std::mutex> lock(idempotency_mutex_);
    const auto it = idempotency_cache_.find(IdempotencyKey(producer_id, sequence));
    if (it != idempotency_cache_.end()) return it->second;
  }
  if (position + 4 > payload.size()) return MakeResponse(request, protocol::Status::kBadRequest);
  const std::uint32_t requested_partition = Get32(payload, position);
  if (!Take(payload, &position, 4)) return MakeResponse(request, protocol::Status::kBadRequest);
  if (!Take(payload, &position, 2)) return MakeResponse(request, protocol::Status::kBadRequest);
  const std::uint16_t key_length = Get16(payload, position - 2);
  const std::size_t key_position = position;
  if (!Take(payload, &position, key_length) || !Take(payload, &position, 4)) {
    return MakeResponse(request, protocol::Status::kBadRequest);
  }
  const std::uint32_t value_length = Get32(payload, position - 4);
  const std::size_t value_position = position;
  if (!Take(payload, &position, value_length) || position != payload.size()) {
    return MakeResponse(request, protocol::Status::kBadRequest);
  }
  std::uint32_t partition = 0;
  std::string error;
  const std::string key(payload.substr(key_position, key_length));
  if (!queues_.ResolvePartition(request.topic, requested_partition, key, &partition, &error)) {
    return MakeResponse(request, error == "unknown topic" ? protocol::Status::kUnknownTopic
                                                           : protocol::Status::kBadRequest);
  }
  if (enforce_topic_quota && (request.flags & protocol::kFlagReplication) == 0 &&
      !AllowTopicBytes(request.topic, static_cast<std::uint64_t>(key_length) + value_length))
    return MakeResponse(request, protocol::Status::kQuotaExceeded);
  core::Message message;
  if (!storage_.Append(request.topic, partition, key, std::string(payload.substr(value_position, value_length)),
                       &message, &error)) {
    return MakeResponse(request, protocol::Status::kStorageError);
  }
  replication_coordinator_->RecordLocalOffset(message.offset);
  if ((request.flags & protocol::kAckMask) == protocol::kAckAll && !Replicate(request.topic, partition, message))
    return MakeResponse(request, protocol::Status::kStorageError);
  std::string response_payload;
  Put32(&response_payload, partition);
  Put64(&response_payload, message.offset);
  auto response = MakeResponse(request, protocol::Status::kOk, std::move(response_payload));
  if (has_metadata) { std::lock_guard<std::mutex> lock(idempotency_mutex_); idempotency_cache_[IdempotencyKey(producer_id, sequence)] = response; }
  return response;
}

bool Broker::Replicate(const std::string& topic, std::uint32_t partition, const core::Message& message) {
  if (replication_peers_.empty() || replication_quorum_ <= 1) return false;
  std::size_t acknowledgements = 1;
  for (const auto& peer : replication_peers_) {
    ReplicationClient client(peer.host, peer.port);
    if (client.Append(topic, partition, std::vector<core::Message>{message}, replication_coordinator_->term(), replication_coordinator_->commitIndex(), node_id_)) {
      ++acknowledgements;
      replication_coordinator_->ObserveHeartbeat(peer.node_id, message.offset);
    }
  }
  if (acknowledgements < replication_quorum_) return false;
  return replication_coordinator_->AdvanceCommit(message.offset);
}

protocol::Response Broker::HandleProduceBatch(const protocol::Request& request) {
  if ((request.flags & protocol::kFlagReplication) == 0 &&
      replication_coordinator_->role() != ReplicaRole::kLeader)
    return MakeResponse(request, protocol::Status::kNotLeader);
  if ((request.flags & protocol::kFlagReplication) == 0 && !AllowProduceRequest())
    return MakeResponse(request, protocol::Status::kRateLimited);
  if (!(request.flags & protocol::kFlagProducerMetadata)) return MakeResponse(request, protocol::Status::kNotSupported);
  std::string_view payload = request.payload; std::size_t position = 0;
  if (!Take(payload, &position, 20)) return MakeResponse(request, protocol::Status::kBadRequest);
  const auto producer_id = Get64(payload, 0); const auto first_sequence = Get64(payload, 8); const auto count = Get32(payload, 16);
  if (count == 0 || count > 10000) return MakeResponse(request, protocol::Status::kBadRequest);
  std::vector<protocol::Response> results(count); std::vector<bool> is_cached(count, false);
  {
    std::lock_guard<std::mutex> lock(idempotency_mutex_);
    for (std::uint32_t i = 0; i < count; ++i) { const auto it = idempotency_cache_.find(IdempotencyKey(producer_id, first_sequence + i)); if (it != idempotency_cache_.end()) { results[i] = it->second; is_cached[i] = true; } }
  }
  std::vector<protocol::Request> pending(count);
  std::uint64_t new_bytes = 0;
  for (std::uint32_t i = 0; i < count; ++i) {
    if (!Take(payload, &position, 2)) return MakeResponse(request, protocol::Status::kBadRequest);
    const auto key_size = Get16(payload, position - 2); const auto key_position = position;
    if (!Take(payload, &position, key_size) || !Take(payload, &position, 4)) return MakeResponse(request, protocol::Status::kBadRequest);
    const auto value_size = Get32(payload, position - 4); const auto value_position = position;
    if (!Take(payload, &position, value_size)) return MakeResponse(request, protocol::Status::kBadRequest);
    if (is_cached[i]) { if (results[i].payload.size() != 12) return MakeResponse(request, protocol::Status::kInternalError); continue; }
    protocol::Request single = request; single.command = protocol::Command::kProduce; single.flags = (request.flags & ~protocol::kFlagProducerMetadata) | protocol::kFlagProducerMetadata;
    std::string single_payload; Put64(&single_payload, producer_id); Put64(&single_payload, first_sequence + i); Put32(&single_payload, 0xFFFFFFFFu); Put16(&single_payload, key_size); single_payload.append(payload.substr(key_position, key_size)); Put32(&single_payload, value_size); single_payload.append(payload.substr(value_position, value_size)); single.payload = std::move(single_payload);
    pending[i] = std::move(single);
    const auto message_bytes = static_cast<std::uint64_t>(key_size) + value_size;
    if (new_bytes > std::numeric_limits<std::uint64_t>::max() - message_bytes)
      return MakeResponse(request, protocol::Status::kBadRequest);
    new_bytes += message_bytes;
  }
  if (position != payload.size()) return MakeResponse(request, protocol::Status::kBadRequest);
  if ((request.flags & protocol::kFlagReplication) == 0 && !AllowTopicBytes(request.topic, new_bytes))
    return MakeResponse(request, protocol::Status::kQuotaExceeded);
  std::string response_payload; Put32(&response_payload, count);
  for (std::uint32_t i = 0; i < count; ++i) {
    if (!is_cached[i]) {
      results[i] = HandleProduce(pending[i], false, false);
      if (results[i].status != protocol::Status::kOk) return MakeResponse(request, results[i].status);
    }
    response_payload.append(results[i].payload);
  }
  return MakeResponse(request, protocol::Status::kOk, std::move(response_payload));
}

protocol::Response Broker::HandleFetch(const protocol::Request& request) {
  if (request.payload.size() != 16) return MakeResponse(request, protocol::Status::kBadRequest);
  const std::uint32_t partition = Get32(request.payload, 0);
  const std::uint64_t offset = Get64(request.payload, 4);
  const std::uint32_t max_bytes = Get32(request.payload, 12);
  if (max_bytes == 0) return MakeResponse(request, protocol::Status::kBadRequest);
  std::uint32_t resolved_partition = 0;
  std::string error;
  if (!queues_.ResolvePartition(request.topic, partition, "", &resolved_partition, &error)) {
    return MakeResponse(request, error == "unknown topic" ? protocol::Status::kUnknownTopic
                                                           : protocol::Status::kInvalidOffset);
  }
  std::vector<core::Message> messages;
  if (!storage_.Read(request.topic, resolved_partition, offset, max_bytes, &messages, &error)) {
    return MakeResponse(request, protocol::Status::kStorageError);
  }
  std::string response_payload;
  Put32(&response_payload, 0);
  std::uint32_t response_count = 0;
  for (const auto& message : messages) {
    const auto record_size = 8ULL + 8ULL + 2ULL + message.key.size() + 4ULL + message.value.size();
    if (record_size > protocol::kMaxPayloadBytes - 4 ||
        response_payload.size() > protocol::kMaxPayloadBytes - 4 - record_size) {
      break;
    }
    Put64(&response_payload, message.offset);
    Put64(&response_payload, static_cast<std::uint64_t>(message.timestamp_ms));
    Put16(&response_payload, static_cast<std::uint16_t>(message.key.size()));
    response_payload.append(message.key);
    Put32(&response_payload, static_cast<std::uint32_t>(message.value.size()));
    response_payload.append(message.value);
    ++response_count;
  }
  response_payload[0] = static_cast<char>(response_count >> 24);
  response_payload[1] = static_cast<char>(response_count >> 16);
  response_payload[2] = static_cast<char>(response_count >> 8);
  response_payload[3] = static_cast<char>(response_count);
  return MakeResponse(request, protocol::Status::kOk, std::move(response_payload));
}

protocol::Response Broker::MakeResponse(const protocol::Request& request, protocol::Status status,
                                        std::string payload) const {
  protocol::Response response;
  response.status = status;
  response.request_id = request.request_id;
  response.payload = std::move(payload);
  return response;
}

}  // namespace mq::server
