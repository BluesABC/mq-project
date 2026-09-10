#pragma once

#include <cstdint>
#include <string>

namespace mq::protocol {

/**
 * @brief 协议常量
 *
 * 集中定义协议相关的常量，避免客户端和 Broker 对帧格式、版本或长度上限产生分歧。
 *
 * 设计要点：
 * - 常量集中管理：便于协议升级和兼容性维护
 * - 魔数校验：防止非法连接和数据损坏
 * - 版本管理：支持协议演进
 */
constexpr std::uint16_t kMagic = 0x4D51;                 ///< 魔数 "MQ"，用于帧头校验
constexpr std::uint8_t kCurrentVersion = 1;              ///< 当前协议版本
constexpr std::uint32_t kMaxPayloadBytes = 1024 * 1024;  ///< 最大载荷 1MB，防止 OOM

/**
 * @brief 命令类型枚举
 *
 * 定义所有支持的请求命令。
 * 编号设计：
 * - 0x01-0x0F：管理命令（Topic 管理）
 * - 0x10-0x1F：生产命令
 * - 0x20-0x2F：消费命令
 * - 0x30-0x3F：协调命令（消费者组）
 * - 0x40-0x4F：复制命令
 */
enum class Command : std::uint8_t {
  // 管理命令
  kCreateTopic = 0x01,  ///< 创建 Topic
  kDeleteTopic = 0x02,  ///< 删除 Topic
  kListTopic = 0x03,    ///< 列出所有 Topic
  kMetrics = 0x04,      ///< 获取系统指标

  // 生产命令
  kProduce = 0x10,       ///< 单条消息生产
  kProduceBatch = 0x11,  ///< 批量消息生产

  // 消费命令
  kSyncGroup = 0x20,     ///< 同步消费者组分配
  kOffsetFetch = 0x21,   ///< 获取消费偏移量
  kFetch = 0x22,         ///< 拉取消息
  kCommitOffset = 0x23,  ///< 提交消费偏移量
  kJoinGroup = 0x24,     ///< 加入消费者组

  // 协调命令
  kHeartbeat = 0x30,  ///< 心跳包

  // 复制命令
  kReplicaFetch = 0x40,   ///< 副本拉取日志
  kReplicaAppend = 0x41,  ///< 副本追加日志
  kReplicaVote = 0x42,    ///< 副本投票（Leader 选举）
};

/**
 * @brief 状态码枚举
 *
 * 由协议层统一承载，业务层不应直接返回魔法数字。
 * 状态码设计：
 * - 0x00：成功
 * - 0x10-0x1F：客户端错误（请求无效）
 * - 0x20-0x2F：服务器错误（内部故障）
 */
enum class Status : std::uint8_t {
  kOk = 0x00,                 ///< 成功
  kBadRequest = 0x10,         ///< 请求格式错误
  kUnknownTopic = 0x11,       ///< Topic 不存在
  kTopicExists = 0x12,        ///< Topic 已存在
  kInvalidOffset = 0x13,      ///< 偏移量无效
  kStorageError = 0x14,       ///< 存储错误
  kVersionMismatch = 0x15,    ///< 协议版本不兼容
  kNotSupported = 0x16,       ///< 不支持的操作
  kNotLeader = 0x17,          ///< 不是 Leader 节点
  kRateLimited = 0x18,        ///< 请求频率超限
  kQuotaExceeded = 0x19,      ///< 配额超限
  kResourceExhausted = 0x1A,  ///< 资源耗尽
  kUnauthenticated = 0x1B,    ///< 未认证
  kPermissionDenied = 0x1C,   ///< 权限不足
  kInternalError = 0x20,      ///< 内部错误
};

/**
 * @brief ACK 模式掩码
 *
 * 控制生产请求的确认级别：
 * - kAckZero：不等待确认（最快）
 * - kAckOne：等待 Leader 确认（平衡）
 * - kAckAll：等待所有副本确认（最可靠）
 */
constexpr std::uint16_t kAckMask = 0x0003;  ///< ACK 模式掩码
constexpr std::uint16_t kAckZero = 0x0000;  ///< 不等待确认
constexpr std::uint16_t kAckOne = 0x0001;   ///< 等待 Leader 确认
constexpr std::uint16_t kAckAll = 0x0002;   ///< 等待所有副本确认

/**
 * @brief 标志位
 *
 * 请求/响应中的标志位，用于扩展功能：
 * - kFlagProducerMetadata：携带 Producer 元数据
 * - kFlagReplication：复制请求
 * - kFlagReplicationTerm：复制任期
 * - kFlagAuthentication：携带认证 Token
 */
constexpr std::uint16_t kFlagProducerMetadata = 0x0004;  ///< Producer 元数据标志
constexpr std::uint16_t kFlagReplication = 0x0008;       ///< 复制请求标志
constexpr std::uint16_t kFlagReplicationTerm = 0x0010;   ///< 复制任期标志
constexpr std::uint16_t kFlagAuthentication = 0x0020;    ///< 认证标志

/**
 * @brief 协议请求结构体
 *
 * 所有客户端请求的统一格式。
 *
 * 帧布局：
 * ```
 * [Magic(2)][Version(1)][Command(1)][RequestId(8)]
 * [Flags(2)][TopicLen(2)][Topic(N)]
 * [PayloadLen(4)][Payload(M)]
 * ```
 *
 * 设计要点：
 * - request_id 用于匹配异步响应和幂等处理
 * - flags 用于扩展功能，保持向后兼容
 * - topic 和 payload 长度可变，支持灵活的数据格式
 */
struct Request {
  /**
   * @brief 协议版本
   * 用于版本协商和兼容性检查
   */
  std::uint8_t version = kCurrentVersion;

  /**
   * @brief 命令类型
   */
  Command command = Command::kHeartbeat;

  /**
   * @brief 请求 ID
   * 客户端生成的唯一标识，用于匹配响应和幂等处理
   */
  std::uint64_t request_id = 0;

  /**
   * @brief 标志位
   * 组合使用，表示请求的附加特性
   */
  std::uint16_t flags = 0;

  /**
   * @brief Topic 名称
   * 部分命令需要指定 Topic
   */
  std::string topic;

  /**
   * @brief 请求载荷
   * 根据命令类型不同，载荷格式不同
   */
  std::string payload;
};

/**
 * @brief 协议响应结构体
 *
 * 所有服务器响应的统一格式。
 *
 * 帧布局：
 * ```
 * [Magic(2)][Version(1)][Status(1)][RequestId(8)][Flags(2)][PayloadLen(4)][Payload(M)]
 * ```
 *
 * 设计要点：
 * - 保持 request_id 与请求一致，便于客户端匹配
 * - status 表示操作结果
 * - payload 包含响应数据或错误详情
 */
struct Response {
  /**
   * @brief 协议版本
   */
  std::uint8_t version = kCurrentVersion;

  /**
   * @brief 状态码
   */
  Status status = Status::kOk;

  /**
   * @brief 请求 ID
   * 与对应的 Request 一致
   */
  std::uint64_t request_id = 0;

  /**
   * @brief 标志位
   */
  std::uint16_t flags = 0;

  /**
   * @brief 响应载荷
   * 根据命令类型不同，载荷格式不同
   */
  std::string payload;
};

}  // namespace mq::protocol