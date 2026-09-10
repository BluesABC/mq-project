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

#include "mq/core/consumer_group_coordinator.h"
#include "mq/core/consumer_offset_store.h"
#include "mq/core/queue_manager.h"
#include "mq/core/storage_engine.h"
#include "mq/core/topic_metadata_store.h"
#include "mq/protocol/commands.h"

namespace mq::server {

/**
 * @brief 复制对等节点信息
 *
 * 描述集群中的一个节点，用于主从复制和高可用。
 */
struct ReplicationPeer {
  std::string node_id;     ///< 节点唯一标识
  std::string host;        ///< 节点 IP 地址
  std::uint16_t port = 0;  ///< 节点端口
  bool leader = false;     ///< 是否为 Leader
};

/**
 * @brief 客户端授权信息
 *
 * 同一客户端 Token 共享一套 ACL。
 * 空的 Topic 列表表示不限制 Topic。
 */
struct ClientAuthorization {
  bool allow_admin = true;                  ///< 是否允许管理操作
  bool allow_produce = true;                ///< 是否允许生产消息
  bool allow_consume = true;                ///< 是否允许消费消息
  std::vector<std::string> produce_topics;  ///< 允许生产的 Topic 列表
  std::vector<std::string> consume_topics;  ///< 允许消费的 Topic 列表
};

/**
 * @brief 消息队列 Broker
 *
 * 编排协议、路由、存储、位点和复制模块，是请求进入业务层的唯一入口。
 *
 * 在整体架构中的角色：
 * - 接收 TcpServer 解码后的 Request
 * - 根据命令类型分发到对应的 Handler
 * - 调用底层模块（StorageEngine、QueueManager 等）执行业务逻辑
 * - 返回 Response 给 TcpServer 发送
 *
 * 核心功能：
 * 1. Topic 管理：创建、删除、列表
 * 2. 消息生产：单条、批量、ACK 模式
 * 3. 消息消费：拉取、提交偏移量
 * 4. 消费者组：加入、同步、心跳
 * 5. 高可用：主从复制、Leader 选举
 * 6. 安全：认证、授权、限流、配额
 *
 * 与其他模块的关系：
 * - TcpServer 调用 Handle 方法处理请求
 * - StorageEngine 负责消息持久化
 * - QueueManager 负责元数据管理
 * - ConsumerGroupCoordinator 负责消费者组协调
 * - ReplicationCoordinator 负责主从复制
 *
 * 线程安全性：
 * - Handle 方法可从多个 EventLoop 线程并发调用
 * - 内部使用细粒度锁保护共享状态
 *
 * 设计要点：
 * - 幂等处理：通过 request_id 防止重复请求
 * - 背压控制：通过限流和配额防止过载
 * - 故障恢复：通过 WAL 和偏移量持久化保证数据一致性
 *
 * 【建议】当前实现是单机版本，分布式版本需要考虑：
 * - 分布式事务
 * - Leader 选举（Raft/Paxos）
 * - 跨节点复制
 */
class Broker {
 public:
  /**
   * @brief 构造函数，使用默认存储配置
   *
   * @param data_dir 数据存储目录
   */
  explicit Broker(std::filesystem::path data_dir);

  /**
   * @brief 构造函数，使用自定义存储配置
   *
   * @param data_dir 数据存储目录
   * @param storage_config 存储配置
   */
  Broker(std::filesystem::path data_dir, core::StorageConfig storage_config);

  /**
   * @brief 析构函数，优雅关闭 Broker
   */
  ~Broker();

  /**
   * @brief 打开 Broker，初始化所有组件
   *
   * 初始化流程：
   * 1. 打开 StorageEngine
   * 2. 加载 Topic 元数据
   * 3. 加载消费者偏移量
   * 4. 启动后台清理线程
   *
   * @param error 可选的错误信息输出参数
   * @return true 初始化成功；false 存储错误或配置错误
   */
  bool Open(std::string* error = nullptr);

  /**
   * @brief 配置主从复制
   *
   * @param node_id 当前节点 ID
   * @param peers 对等节点列表
   * @param quorum 复制 quorum（默认为节点数的一半 + 1）
   * @param follower 是否为 Follower 模式
   * @param auth_token 复制认证 Token
   */
  void ConfigureReplication(std::string node_id, std::vector<ReplicationPeer> peers,
                            std::size_t quorum = 0, bool follower = false,
                            std::string auth_token = {});

  /**
   * @brief 配置客户端认证
   *
   * @param auth_token 认证 Token
   * @param authorization 授权信息
   */
  void ConfigureClientAuth(std::string auth_token, ClientAuthorization authorization = {});

  /**
   * @brief 配置生产限流
   *
   * @param produce_requests_per_second 每秒允许的请求数（0 表示不限流）
   */
  void ConfigureRateLimit(std::uint64_t produce_requests_per_second);

  /**
   * @brief 配置 Topic 级别配额
   *
   * @param produce_bytes_per_second 每秒允许的字节数（0 表示不限制）
   */
  void ConfigureTopicQuota(std::uint64_t produce_bytes_per_second);

  /**
   * @brief 启动复制线程
   */
  void StartReplication();

  /**
   * @brief 停止复制线程
   */
  void StopReplication();

  /**
   * @brief 强制刷盘
   *
   * 将所有未持久化的数据写入磁盘。
   *
   * @param error 可选的错误信息输出参数
   * @return true 刷盘成功；false IO 错误
   */
  bool Flush(std::string* error = nullptr);

  /**
   * @brief 处理协议请求
   *
   * 这是请求处理的主入口，根据命令类型分发到对应的 Handler。
   * 保持 request_id 原样返回，便于客户端匹配响应和处理重试。
   *
   * @param request 解码后的协议请求
   * @return 协议响应
   */
  protocol::Response Handle(const protocol::Request& request);

 private:
  /**
   * @brief 处理创建 Topic 请求
   */
  protocol::Response HandleCreateTopic(const protocol::Request& request);

  /**
   * @brief 处理列出 Topic 请求
   */
  protocol::Response HandleListTopic(const protocol::Request& request);

  /**
   * @brief 处理获取指标请求
   */
  protocol::Response HandleMetrics(const protocol::Request& request);

  /**
   * @brief 处理删除 Topic 请求
   */
  protocol::Response HandleDeleteTopic(const protocol::Request& request);

  /**
   * @brief 处理生产消息请求
   *
   * @param request 请求
   * @param enforce_rate_limit 是否执行限流检查
   * @param enforce_topic_quota 是否执行配额检查
   */
  protocol::Response HandleProduce(const protocol::Request& request, bool enforce_rate_limit = true,
                                   bool enforce_topic_quota = true);

  /**
   * @brief 处理批量生产请求
   */
  protocol::Response HandleProduceBatch(const protocol::Request& request);

  /**
   * @brief 处理拉取消息请求
   */
  protocol::Response HandleFetch(const protocol::Request& request);

  /**
   * @brief 处理提交偏移量请求
   */
  protocol::Response HandleCommitOffset(const protocol::Request& request);

  /**
   * @brief 处理心跳请求
   */
  protocol::Response HandleHeartbeat(const protocol::Request& request);

  /**
   * @brief 处理加入消费者组请求
   */
  protocol::Response HandleJoinGroup(const protocol::Request& request);

  /**
   * @brief 处理同步消费者组请求
   */
  protocol::Response HandleSyncGroup(const protocol::Request& request);

  /**
   * @brief 处理获取偏移量请求
   */
  protocol::Response HandleOffsetFetch(const protocol::Request& request);

  /**
   * @brief 处理副本拉取请求
   */
  protocol::Response HandleReplicaFetch(const protocol::Request& request);

  /**
   * @brief 处理副本追加请求
   */
  protocol::Response HandleReplicaAppend(const protocol::Request& request);

  /**
   * @brief 处理副本投票请求
   */
  protocol::Response HandleReplicaVote(const protocol::Request& request);

  /**
   * @brief 构造响应对象
   *
   * 保持 request_id 和 version 与请求一致。
   *
   * @param request 原始请求
   * @param status 状态码
   * @param payload 响应载荷
   * @return 构造好的响应
   */
  protocol::Response MakeResponse(const protocol::Request& request, protocol::Status status,
                                  std::string payload = {}) const;

  /**
   * @brief 执行消息复制
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param message 要复制的消息
   * @return true 复制成功；false 复制失败
   */
  bool Replicate(const std::string& topic, std::uint32_t partition, const core::Message& message);

  /**
   * @brief 检查生产请求是否允许
   *
   * 基于令牌桶算法进行限流。
   *
   * @return true 允许；false 被限流
   */
  bool AllowProduceRequest();

  /**
   * @brief 检查 Topic 级别配额
   *
   * @param topic Topic 名称
   * @param bytes 本次请求的字节数
   * @return true 允许；false 超过配额
   */
  bool AllowTopicBytes(const std::string& topic, std::uint64_t bytes);

  /**
   * @brief 验证复制请求的合法性
   *
   * @param request 请求
   * @param normalized 输出参数，标准化后的请求
   * @return true 验证通过；false 验证失败
   */
  bool ValidateReplicationRequest(const protocol::Request& request,
                                  protocol::Request* normalized) const;

  /**
   * @brief 验证客户端认证
   *
   * @param request 请求
   * @param normalized 输出参数，标准化后的请求
   * @return true 认证通过；false 认证失败
   */
  bool ValidateClientAuth(const protocol::Request& request, protocol::Request* normalized) const;

  /**
   * @brief 检查客户端授权
   *
   * @param request 请求
   * @return true 授权通过；false 权限不足
   */
  bool AuthorizeClientRequest(const protocol::Request& request) const;

  /**
   * @brief 检查 Topic 是否在允许列表中
   *
   * @param topics 允许的 Topic 列表（空表示不限制）
   * @param topic 目标 Topic
   * @return true 允许；false 不允许
   */
  static bool TopicAllowed(const std::vector<std::string>& topics, const std::string& topic);

  /**
   * @brief 生成分区唯一键
   *
   * 用于复制偏移量跟踪和幂等处理。
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @return 唯一键字符串
   */
  static std::string PartitionKey(const std::string& topic, std::uint32_t partition);

  // ==================== 成员变量 ====================

  core::StorageEngine storage_;                     ///< 存储引擎
  std::filesystem::path data_dir_;                  ///< 数据目录
  core::TopicMetadataStore metadata_store_;         ///< Topic 元数据存储
  core::ConsumerOffsetStore offset_store_;          ///< 消费者偏移量存储
  core::ConsumerGroupCoordinator consumer_groups_;  ///< 消费者组协调器
  core::StorageConfig storage_config_;              ///< 存储配置

  std::vector<core::ConsumerOffset> consumer_offsets_;  ///< 内存中的偏移量缓存

  core::QueueManager queues_;  ///< 队列管理器

  /**
   * @brief Topic 元数据互斥锁
   * 保护 metadata_store_ 和 queues_ 的一致性
   */
  std::mutex topic_metadata_mutex_;

  bool opened_ = false;  ///< 是否已打开

  /**
   * @brief 幂等缓存互斥锁
   */
  std::mutex idempotency_mutex_;

  /**
   * @brief 幂等缓存条目
   */
  struct IdempotencyEntry {
    protocol::Response response;                       ///< 缓存的响应
    std::chrono::steady_clock::time_point expires_at;  ///< 过期时间
  };

  /**
   * @brief 幂等缓存
   * 用于防止重复请求，key 为 request_id 的字符串表示
   */
  std::unordered_map<std::string, IdempotencyEntry> idempotency_cache_;

  // ==================== 复制相关 ====================

  std::string node_id_ = "node-local";              ///< 当前节点 ID
  std::vector<ReplicationPeer> replication_peers_;  ///< 复制对等节点
  std::string replication_auth_token_;              ///< 复制认证 Token
  std::string client_auth_token_;                   ///< 客户端认证 Token
  ClientAuthorization client_authorization_;        ///< 客户端授权信息

  bool replication_configured_ = false;  ///< 是否配置了复制
  std::size_t replication_quorum_ = 0;   ///< 复制 quorum

  std::unique_ptr<class ReplicationCoordinator> replication_coordinator_;  ///< 复制协调器

  bool follower_ = false;                      ///< 是否为 Follower 模式
  std::atomic<bool> stop_replication_{false};  ///< 停止复制标志
  std::condition_variable replication_cv_;     ///< 复制线程条件变量
  std::mutex replication_mutex_;               ///< 复制互斥锁
  std::thread replication_thread_;             ///< 复制线程

  /**
   * @brief 复制偏移量跟踪
   * 记录每个分区的下一个待复制 offset
   */
  std::unordered_map<std::string, std::uint64_t> replication_offsets_;

  // ==================== 指标统计 ====================

  std::atomic<std::uint64_t> request_count_{0};  ///< 总请求数
  std::atomic<std::uint64_t> produce_count_{0};  ///< 生产请求数
  std::atomic<std::uint64_t> fetch_count_{0};    ///< 消费请求数
  std::atomic<std::uint64_t> error_count_{0};    ///< 错误请求数

  // ==================== 限流相关 ====================

  /**
   * @brief 限流互斥锁
   */
  std::mutex rate_limit_mutex_;

  /**
   * @brief 生产请求限流（每秒请求数）
   * 0 表示不限流
   */
  std::uint64_t produce_rate_limit_ = 0;

  /**
   * @brief 令牌桶当前令牌数
   */
  long double produce_tokens_ = 0;

  /**
   * @brief 上次令牌填充时间
   */
  std::chrono::steady_clock::time_point produce_last_refill_ = std::chrono::steady_clock::now();

  /**
   * @brief Topic 配额窗口
   */
  struct TopicQuotaWindow {
    std::uint64_t bytes = 0;  ///< 窗口内累计字节数
    std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();  ///< 窗口开始时间
  };

  /**
   * @brief 配额互斥锁
   */
  std::mutex topic_quota_mutex_;

  /**
   * @brief Topic 级别配额（每秒字节数）
   * 0 表示不限制
   */
  std::uint64_t topic_produce_quota_ = 0;

  /**
   * @brief 各 Topic 的配额窗口
   */
  std::unordered_map<std::string, TopicQuotaWindow> topic_quota_windows_;

  /**
   * @brief 复制线程主函数
   */
  void ReplicationLoop();
};

}  // namespace mq::server