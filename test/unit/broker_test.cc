/**
 * @file broker_test.cc
 * @brief Broker模块单元测试
 * 
 * 本文件包含对消息队列Broker核心功能的单元测试，验证以下功能：
 * 1. 主题(Topic)的创建、删除和元数据持久化
 * 2. 消息的生产(Produce)和消费(Fetch)流程
 * 3. 主题分区(Partition)管理
 * 4. 消费者偏移量(Offset)提交与恢复
 * 5. 幂等Producer去重机制
 * 6. 批量消息生产
 * 7. 请求频率限制(Rate Limiting)
 * 8. 主题配额(Quota)管理
 * 9. 复制(Replication)认证与数据同步
 * 10. 客户端认证与授权(ACL)
 * 11. 协议版本兼容性检查
 * 12. 获取响应大小限制
 * 
 * 测试使用Google Test风格的断言验证Broker行为的正确性。
 */

#include "mq/server/broker.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

/**
 * @brief 将16位整数以大端字节序写入字符串
 * @param out 目标字符串
 * @param value 要写入的16位无符号整数
 */
void Put16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}

/**
 * @brief 将32位整数以大端字节序写入字符串
 * @param out 目标字符串
 * @param value 要写入的32位无符号整数
 */
void Put32(std::string* out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}

/**
 * @brief 将64位整数以大端字节序写入字符串
 * @param out 目标字符串
 * @param value 要写入的64位无符号整数
 */
void Put64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}

/**
 * @brief 从字符串中读取16位大端字节序整数
 * @param input 源字符串
 * @param position 起始位置
 * @return 读取的16位无符号整数
 */
std::uint16_t Get16(const std::string& input, std::size_t position) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(input[position])) << 8) |
         static_cast<unsigned char>(input[position + 1]);
}

/**
 * @brief 从字符串中读取32位大端字节序整数
 * @param input 源字符串
 * @param position 起始位置
 * @return 读取的32位无符号整数
 */
std::uint32_t Get32(const std::string& input, std::size_t position) {
  std::uint32_t value = 0;
  for (int index = 0; index < 4; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}

/**
 * @brief 从字符串中读取64位大端字节序整数
 * @param input 源字符串
 * @param position 起始位置
 * @return 读取的64位无符号整数
 */
std::uint64_t Get64(const std::string& input, std::size_t position) {
  std::uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value = (value << 8) | static_cast<unsigned char>(input[position + index]);
  }
  return value;
}

/**
 * @brief 列出Broker中所有主题
 * @param broker Broker实例指针
 * @return 主题元数据列表
 */
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

/**
 * @brief 创建主题请求（使用默认主题名"orders"和3个分区）
 * @param request_id 请求ID
 * @return 构造的主题创建请求
 */
mq::protocol::Request CreateTopicRequest(std::uint64_t request_id) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kCreateTopic;
  request.request_id = request_id;
  request.topic = "orders";
  Put32(&request.payload, 3);
  return request;
}

/**
 * @brief 创建主题请求（指定主题名和分区数）
 * @param request_id 请求ID
 * @param topic 主题名称
 * @param partitions 分区数量
 * @return 构造的主题创建请求
 */
mq::protocol::Request CreateTopicRequest(std::uint64_t request_id, std::string topic,
                                         std::uint32_t partitions) {
  auto request = CreateTopicRequest(request_id);
  request.topic = std::move(topic);
  request.payload.clear();
  Put32(&request.payload, partitions);
  return request;
}

/**
 * @brief 构造消息生产请求
 * @param request_id 请求ID
 * @param key 消息键
 * @param value 消息值
 * @return 构造的生产请求
 */
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

/**
 * @brief 构造消息获取请求
 * @param request_id 请求ID
 * @param partition 分区编号
 * @return 构造的获取请求
 */
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

/**
 * @brief 构造副本数据拉取请求
 * @param request_id 请求ID
 * @param partition 分区编号
 * @param offset 起始偏移量
 * @return 构造的副本拉取请求
 */
mq::protocol::Request ReplicaFetchRequest(std::uint64_t request_id, std::uint32_t partition,
                                          std::uint64_t offset) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kReplicaFetch;
  request.request_id = request_id;
  request.flags = mq::protocol::kFlagReplication;
  request.topic = "orders";
  Put32(&request.payload, partition);
  Put64(&request.payload, offset);
  Put32(&request.payload, 1024);
  return request;
}

/**
 * @brief 构造副本数据追加请求
 * @param request_id 请求ID
 * @param partition 分区编号
 * @param messages 要追加的消息数据
 * @return 构造的副本追加请求
 */
mq::protocol::Request ReplicaAppendRequest(std::uint64_t request_id, std::uint32_t partition,
                                           const std::string& messages) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kReplicaAppend;
  request.request_id = request_id;
  request.flags = mq::protocol::kFlagReplication;
  request.topic = "orders";
  Put32(&request.payload, partition);
  Put32(&request.payload, Get32(messages, 0));
  request.payload.append(messages, 4, messages.size() - 4);
  std::string term_payload;
  Put64(&term_payload, 1);
  Put64(&term_payload, 0);
  Put16(&term_payload, 6);
  term_payload.append("leader");
  term_payload.append(request.payload);
  request.payload = std::move(term_payload);
  request.flags |= mq::protocol::kFlagReplicationTerm;
  return request;
}

/**
 * @brief 为复制请求添加认证信息
 * @param request 要认证的请求指针
 */
void AuthenticateReplication(mq::protocol::Request* request) {
  assert(request != nullptr);
  std::string prefix;
  Put16(&prefix, 10);
  prefix.append("test-token");
  prefix.append(std::move(request->payload));
  request->payload = std::move(prefix);
}

/**
 * @brief 为客户端请求添加认证信息
 * @param request 要认证的请求指针
 * @param token 客户端认证令牌
 */
void AuthenticateClient(mq::protocol::Request* request, std::string_view token) {
  assert(request != nullptr && token.size() <= 65535);
  std::string prefix;
  Put16(&prefix, static_cast<std::uint16_t>(token.size()));
  prefix.append(token);
  prefix.append(std::move(request->payload));
  request->payload = std::move(prefix);
  request->flags |= mq::protocol::kFlagAuthentication;
}

/**
 * @brief 构造消费者偏移量提交请求
 * @param request_id 请求ID
 * @param group 消费者组名称
 * @param partition 分区编号
 * @param offset 要提交的偏移量
 * @return 构造的偏移量提交请求
 */
mq::protocol::Request CommitRequest(std::uint64_t request_id, std::string group,
                                    std::uint32_t partition, std::uint64_t offset) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kCommitOffset;
  request.request_id = request_id;
  request.topic = "orders";
  Put16(&request.payload, static_cast<std::uint16_t>(group.size()));
  request.payload.append(group);
  Put32(&request.payload, partition);
  Put64(&request.payload, offset);
  return request;
}

/**
 * @brief 构造幂等生产请求（带有Producer元数据）
 * @param request_id 请求ID
 * @return 构造的幂等生产请求
 */
mq::protocol::Request IdempotentProduceRequest(std::uint64_t request_id) {
  auto request = ProduceRequest(request_id, "idem", "once");
  request.flags = mq::protocol::kFlagProducerMetadata | mq::protocol::kAckOne;
  std::string metadata;
  Put64(&metadata, 99);
  Put64(&metadata, 7);
  metadata.append(request.payload);
  request.payload = std::move(metadata);
  return request;
}

/**
 * @brief 构造批量消息生产请求
 * @param request_id 请求ID
 * @param producer_id 生产者ID（用于幂等性）
 * @param first_sequence 第一条消息的序列号
 * @param messages 消息列表，每个元素为(key, value)对
 * @return 构造的批量生产请求
 */
mq::protocol::Request ProduceBatchRequest(
    std::uint64_t request_id, std::uint64_t producer_id, std::uint64_t first_sequence,
    const std::vector<std::pair<std::string, std::string>>& messages) {
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kProduceBatch;
  request.request_id = request_id;
  request.topic = "orders";
  request.flags = mq::protocol::kFlagProducerMetadata | mq::protocol::kAckOne;
  Put64(&request.payload, producer_id);
  Put64(&request.payload, first_sequence);
  Put32(&request.payload, static_cast<std::uint32_t>(messages.size()));
  for (const auto& message : messages) {
    Put16(&request.payload, static_cast<std::uint16_t>(message.first.size()));
    request.payload.append(message.first);
    Put32(&request.payload, static_cast<std::uint32_t>(message.second.size()));
    request.payload.append(message.second);
  }
  return request;
}

/**
 * @brief 测试Broker的核心功能：主题创建、消息生产消费、幂等性、偏移量提交、指标收集、限流和配额
 * 
 * 测试流程：
 * 1. 创建Broker实例并打开
 * 2. 测试主题创建（包括重复创建检测）
 * 3. 测试主题列表查询
 * 4. 测试消息生产和消费
 * 5. 测试幂等Producer去重（包括并发重试）
 * 6. 测试消费者偏移量提交
 * 7. 测试指标(Metrics)收集
 * 8. 测试请求频率限制
 * 9. 测试主题配额管理
 */
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
  const auto idem = broker.Handle(IdempotentProduceRequest(6));
  const auto duplicate_idem = broker.Handle(IdempotentProduceRequest(7));
  assert(idem.status == mq::protocol::Status::kOk &&
         duplicate_idem.status == mq::protocol::Status::kOk);
  assert(idem.payload == duplicate_idem.payload);
  std::vector<std::thread> retries;
  std::vector<mq::protocol::Response> retry_results(8);
  for (std::size_t index = 0; index < retry_results.size(); ++index)
    retries.emplace_back([&, index] { retry_results[index] = broker.Handle(IdempotentProduceRequest(20 + index)); });
  for (auto& retry : retries) retry.join();
  for (const auto& result : retry_results)
    assert(result.status == mq::protocol::Status::kOk && result.payload == idem.payload);
  assert(broker.Handle(CommitRequest(5, "consumer-a", partition, 1)).status ==
         mq::protocol::Status::kOk);
  assert(std::filesystem::exists(root / "metadata" / "consumer_offsets.meta"));
  mq::protocol::Request metrics_request;
  metrics_request.command = mq::protocol::Command::kMetrics;
  const auto metrics = broker.Handle(metrics_request);
  assert(metrics.status == mq::protocol::Status::kOk);
  assert(metrics.payload.find("mq_requests_total ") != std::string::npos);
  assert(metrics.payload.find("mq_produce_total ") != std::string::npos);
  assert(metrics.payload.find("mq_fetch_total 1") != std::string::npos);
  broker.ConfigureRateLimit(1);
  assert(broker.Handle(ProduceRequest(8, "rate-1", "allowed")).status == mq::protocol::Status::kOk);
  assert(broker.Handle(ProduceRequest(9, "rate-2", "limited")).status ==
         mq::protocol::Status::kRateLimited);
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));
  assert(broker.Handle(ProduceRequest(10, "rate-3", "refilled")).status ==
         mq::protocol::Status::kOk);
  broker.ConfigureRateLimit(0);
  broker.ConfigureTopicQuota(7);
  const auto oversized_batch = ProduceBatchRequest(11, 200, 0, {{"a", "123"}, {"b", "123"}});
  assert(broker.Handle(oversized_batch).status == mq::protocol::Status::kQuotaExceeded);
  assert(broker.Handle(ProduceRequest(12, "q", "123")).status == mq::protocol::Status::kOk);
  assert(broker.Handle(ProduceRequest(13, "q", "123")).status ==
         mq::protocol::Status::kQuotaExceeded);
  const auto quota_metrics = broker.Handle(metrics_request);
  assert(quota_metrics.payload.find("mq_topic_produce_quota_bytes_per_second 7") !=
         std::string::npos);
  std::filesystem::remove_all(root, error);
}

/**
 * @brief 测试主题删除功能和协议版本不兼容检测
 * 
 * 测试流程：
 * 1. 创建Broker实例并打开
 * 2. 创建主题并生产消息
 * 3. 删除主题并验证存储文件已被清理
 * 4. 重新创建同名主题并验证数据已清空
 * 5. 测试协议版本不兼容时的错误响应
 */
void DeleteTopicRemovesStorageAndVersionMismatchIsReported() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_delete_topic_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  mq::server::Broker broker(root);
  assert(broker.Open());
  assert(broker.Handle(CreateTopicRequest(1, "reusable", 1)).status == mq::protocol::Status::kOk);
  auto produce = ProduceRequest(2, "key", "value");
  produce.topic = "reusable";
  assert(broker.Handle(produce).status == mq::protocol::Status::kOk);
  mq::protocol::Request remove;
  remove.command = mq::protocol::Command::kDeleteTopic;
  remove.request_id = 3;
  remove.topic = "reusable";
  assert(broker.Handle(remove).status == mq::protocol::Status::kOk);
  assert(!std::filesystem::exists(root / "queues" / "7265757361626c65"));
  assert(broker.Handle(CreateTopicRequest(4, "reusable", 1)).status == mq::protocol::Status::kOk);
  auto fetch = FetchRequest(5, 0);
  fetch.topic = "reusable";
  assert(broker.Handle(fetch).status == mq::protocol::Status::kOk);
  assert(Get32(broker.Handle(fetch).payload, 0) == 0);
  auto unsupported = CreateTopicRequest(6, "reusable-version", 1);
  unsupported.version = 2;
  const auto mismatch = broker.Handle(unsupported);
  assert(mismatch.status == mq::protocol::Status::kVersionMismatch && mismatch.payload.size() == 1 &&
         static_cast<unsigned char>(mismatch.payload[0]) == mq::protocol::kCurrentVersion);
  std::filesystem::remove_all(root, error);
}

/**
 * @brief 测试拒绝未认证的复制请求
 * 
 * 测试流程：
 * 1. 配置Broker为leader模式并设置复制令牌
 * 2. 发送未认证的复制拉取请求，验证被拒绝
 * 3. 添加认证信息后重新发送，验证被接受
 * 4. 发送未认证的复制生产请求，验证被拒绝
 */
void RejectsUnauthenticatedReplication() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_replication_auth_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  mq::server::Broker broker(root);
  assert(broker.Open());
  broker.ConfigureReplication("leader", {}, 0, false, "test-token");
  assert(broker.Handle(CreateTopicRequest(1, "secured", 1)).status == mq::protocol::Status::kOk);
  auto request = ReplicaFetchRequest(2, 0, 0);
  request.topic = "secured";
  assert(broker.Handle(request).status == mq::protocol::Status::kBadRequest);
  AuthenticateReplication(&request);
  assert(broker.Handle(request).status == mq::protocol::Status::kOk);
  auto produce = ProduceRequest(3, "k", "v");
  produce.topic = "secured";
  produce.flags = mq::protocol::kFlagReplication;
  assert(broker.Handle(produce).status == mq::protocol::Status::kBadRequest);
  std::filesystem::remove_all(root, error);
}

/**
 * @brief 测试客户端认证功能
 * 
 * 测试流程：
 * 1. 配置Broker要求客户端认证
 * 2. 发送未认证的请求，验证被拒绝
 * 3. 使用错误令牌认证，验证被拒绝
 * 4. 使用正确令牌认证，验证被接受
 * 5. 测试生产请求的认证
 */
void RequiresClientAuthentication() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_client_auth_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  mq::server::Broker broker(root);
  assert(broker.Open());
  broker.ConfigureClientAuth("client-token");

  auto unauthenticated = CreateTopicRequest(1, "secured-client", 1);
  assert(broker.Handle(unauthenticated).status == mq::protocol::Status::kUnauthenticated);

  auto wrong = unauthenticated;
  AuthenticateClient(&wrong, "wrong-token");
  assert(broker.Handle(wrong).status == mq::protocol::Status::kUnauthenticated);

  auto authenticated = unauthenticated;
  AuthenticateClient(&authenticated, "client-token");
  assert(broker.Handle(authenticated).status == mq::protocol::Status::kOk);

  auto produce = ProduceRequest(2, "key", "value");
  produce.topic = "secured-client";
  AuthenticateClient(&produce, "client-token");
  assert(broker.Handle(produce).status == mq::protocol::Status::kOk);
  std::filesystem::remove_all(root, error);
}

/**
 * @brief 测试客户端授权(ACL)功能
 * 
 * 测试流程：
 * 1. 配置Broker的ACL策略：允许生产到"allowed"主题，禁止消费和管理操作
 * 2. 测试允许的生产操作
 * 3. 测试被禁止的生产操作（不同主题）
 * 4. 测试被禁止的消费操作
 * 5. 测试被禁止的管理操作（列出主题）
 */
void EnforcesClientAuthorization() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_client_acl_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  mq::server::Broker broker(root);
  assert(broker.Open());
  assert(broker.Handle(CreateTopicRequest(1, "allowed", 1)).status == mq::protocol::Status::kOk);
  assert(broker.Handle(CreateTopicRequest(2, "blocked", 1)).status == mq::protocol::Status::kOk);
  mq::server::ClientAuthorization authorization;
  authorization.allow_admin = false;
  authorization.allow_produce = true;
  authorization.allow_consume = false;
  authorization.produce_topics = {"allowed"};
  broker.ConfigureClientAuth("client-token", authorization);

  auto allowed = ProduceRequest(3, "key", "value");
  allowed.topic = "allowed";
  AuthenticateClient(&allowed, "client-token");
  assert(broker.Handle(allowed).status == mq::protocol::Status::kOk);

  auto blocked = allowed;
  blocked.request_id = 4;
  blocked.topic = "blocked";
  assert(broker.Handle(blocked).status == mq::protocol::Status::kPermissionDenied);

  auto fetch = FetchRequest(5, 0);
  fetch.topic = "allowed";
  AuthenticateClient(&fetch, "client-token");
  assert(broker.Handle(fetch).status == mq::protocol::Status::kPermissionDenied);

  auto list = CreateTopicRequest(6);
  list.command = mq::protocol::Command::kListTopic;
  list.topic.clear();
  list.payload.clear();
  AuthenticateClient(&list, "client-token");
  assert(broker.Handle(list).status == mq::protocol::Status::kPermissionDenied);
  std::filesystem::remove_all(root, error);
}

/**
 * @brief 测试主题元数据持久化和恢复
 * 
 * 测试流程：
 * 1. 创建Broker实例并创建多个主题
 * 2. 关闭Broker（触发元数据持久化）
 * 3. 重新打开Broker并验证主题元数据已恢复
 */
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

/**
 * @brief 测试消息复制功能（连续消息的复制）
 * 
 * 测试流程：
 * 1. 创建leader和follower两个Broker实例
 * 2. 配置复制关系
 * 3. 在leader上创建主题并生产消息
 * 4. 从leader拉取数据并追加到follower
 * 5. 验证follower上的数据与leader一致
 * 6. 测试重复追加时的偏移量错误检测
 */
void ReplicatesContiguousMessages() {
  const auto leader_root = std::filesystem::temp_directory_path() / "mq_project_leader_test";
  const auto follower_root = std::filesystem::temp_directory_path() / "mq_project_follower_test";
  std::error_code error;
  std::filesystem::remove_all(leader_root, error);
  std::filesystem::remove_all(follower_root, error);
  mq::server::Broker leader(leader_root);
  mq::server::Broker follower(follower_root);
  assert(leader.Open() && follower.Open());
  leader.ConfigureReplication("leader", {}, 0, false, "test-token");
  follower.ConfigureReplication("follower", {{"leader", "127.0.0.1", 0, true}}, 2, true,
                                "test-token");
  assert(leader.Handle(CreateTopicRequest(1)).status == mq::protocol::Status::kOk);
  assert(follower.Handle(CreateTopicRequest(1)).status == mq::protocol::Status::kOk);
  const auto produced = leader.Handle(ProduceRequest(2, "replica-key", "replica-value"));
  assert(produced.status == mq::protocol::Status::kOk);
  const auto partition = Get32(produced.payload, 0);
  auto replica_fetch = ReplicaFetchRequest(3, partition, 0);
  AuthenticateReplication(&replica_fetch);
  const auto fetched = leader.Handle(replica_fetch);
  assert(fetched.status == mq::protocol::Status::kOk && Get32(fetched.payload, 0) == 1);
  auto replica_append = ReplicaAppendRequest(4, partition, fetched.payload);
  AuthenticateReplication(&replica_append);
  assert(follower.Handle(replica_append).status == mq::protocol::Status::kOk);
  const auto follower_fetch = follower.Handle(FetchRequest(5, partition));
  assert(follower_fetch.status == mq::protocol::Status::kOk &&
         Get32(follower_fetch.payload, 0) == 1);
  auto duplicate_append = ReplicaAppendRequest(6, partition, fetched.payload);
  AuthenticateReplication(&duplicate_append);
  assert(follower.Handle(duplicate_append).status ==
         mq::protocol::Status::kInvalidOffset);
  std::filesystem::remove_all(leader_root, error);
  std::filesystem::remove_all(follower_root, error);
}

/**
 * @brief 测试获取响应大小限制（不超过协议最大负载）
 * 
 * 测试流程：
 * 1. 创建Broker并生产大量消息（5000条，每条256字节）
 * 2. 发送获取请求，请求最大1MB数据
 * 3. 验证响应大小不超过协议最大负载限制
 * 4. 验证至少返回了一条消息
 */
void LimitsFetchResponseToProtocolPayload() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_fetch_limit_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  mq::server::Broker broker(root);
  assert(broker.Open());
  assert(broker.Handle(CreateTopicRequest(1, "fetch-limit", 1)).status ==
         mq::protocol::Status::kOk);

  for (std::uint32_t index = 0; index < 5000; ++index) {
    auto request = ProduceRequest(index + 2, "key", std::string(256, 'x'));
    request.topic = "fetch-limit";
    assert(broker.Handle(request).status == mq::protocol::Status::kOk);
  }

  auto request = FetchRequest(6000, 0);
  request.topic = "fetch-limit";
  request.payload.clear();
  Put32(&request.payload, 0);
  Put64(&request.payload, 0);
  Put32(&request.payload, 1024 * 1024);
  const auto response = broker.Handle(request);
  assert(response.status == mq::protocol::Status::kOk);
  assert(response.payload.size() <= mq::protocol::kMaxPayloadBytes);
  assert(Get32(response.payload, 0) > 0);
  std::filesystem::remove_all(root, error);
}

}  // namespace

/**
 * @brief 主函数，运行所有Broker单元测试
 * @return 0 表示测试成功
 */
int main() {
  CreateProduceFetch();
  RestoresTopicMetadata();
  ReplicatesContiguousMessages();
  LimitsFetchResponseToProtocolPayload();
  DeleteTopicRemovesStorageAndVersionMismatchIsReported();
  RejectsUnauthenticatedReplication();
  RequiresClientAuthentication();
  EnforcesClientAuthorization();
  return 0;
}
