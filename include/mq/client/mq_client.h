#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "mq/core/storage_engine.h"
#include "mq/core/consumer_group_coordinator.h"
#include "mq/network/tls.h"

namespace mq::client {

/**
 * @brief ACK 模式枚举
 *
 * 控制生产请求的确认级别：
 * - kZero：不等待确认，最快但可能丢失
 * - kOne：等待 Leader 确认，平衡可靠性和性能
 * - kAll：等待所有副本确认，最可靠但最慢
 */
enum class AckMode { kZero, kOne, kAll };

/**
 * @brief 生产结果
 *
 * 包含消息写入的位置信息。
 */
struct ProduceResult {
  std::uint32_t partition = 0;  ///< 分区号
  std::uint64_t offset = 0;     ///< 消息偏移量
};

/**
 * @brief 生产者消息
 *
 * 包含消息的键值对。
 */
struct ProducerMessage {
  std::string key;    ///< 消息键
  std::string value;  ///< 消息值
};

/**
 * @brief Topic 信息
 *
 * 包含 Topic 的名称和分区数量。
 */
struct TopicInfo {
  std::string name;             ///< Topic 名称
  std::uint32_t partitions = 0;  ///< 分区数量
};

/**
 * @brief 消息生产者客户端
 *
 * 提供消息生产的高层 API，隐藏协议细节和网络实现。
 *
 * 功能：
 * - 连接 Broker
 * - 创建 Topic
 * - 生产单条/批量消息
 * - 获取系统指标
 *
 * 设计要点：
 * 1. 封装协议帧：调用方只需提供消息内容
 * 2. 自动重连：网络断开时自动重连
 * 3. 异步支持：可选 ACK 模式
 * 4. 错误处理：通过 lastError 获取错误详情
 *
 * 使用示例：
 * @code
 * MqProducer producer;
 * producer.connect("localhost", 9000);
 *
 * ProduceResult result;
 * producer.produce("my-topic", "key1", "value1", AckMode::kOne, &result);
 *
 * std::cout << "写入分区 " << result.partition << "，offset " << result.offset << std::endl;
 * @endcode
 *
 * 线程安全性：
 * - 单个实例不能并发使用
 * - 多个实例可以并发使用
 *
 * 注意事项：
 * - 连接失败时 lastError 会包含错误信息
 * - 发送队列满时 produce 会失败
 * - 必须在析构前调用 close()
 */
class MqProducer {
 public:
  /**
   * @brief 构造函数
   */
  MqProducer();

  /**
   * @brief 析构函数，自动关闭连接
   */
  ~MqProducer();

  // 禁用拷贝
  MqProducer(const MqProducer&) = delete;

  /**
   * @brief 连接到单个 Broker
   *
   * @param host Broker 地址
   * @param port Broker 端口
   * @return true 连接成功；false 连接失败
   */
  bool connect(const std::string& host, std::uint16_t port);

  /**
   * @brief 连接到多个 Broker（自动选择）
   *
   * @param endpoints Broker 地址列表
   * @return true 连接成功；false 所有 Broker 都不可用
   */
  bool connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints);

  /**
   * @brief 创建 Topic
   *
   * @param name Topic 名称
   * @param partitions 分区数量
   * @return true 创建成功；false Topic 已存在或参数无效
   */
  bool createTopic(const std::string& name, std::uint32_t partitions = 1);

  /**
   * @brief 生产单条消息
   *
   * SDK 隐藏协议帧和重连细节，失败原因通过 lastError 获取。
   *
   * @param topic Topic 名称
   * @param key 消息键
   * @param value 消息值
   * @return true 生产成功；false 发送失败
   */
  bool produce(const std::string& topic, const std::string& key, const std::string& value);

  /**
   * @brief 生产单条消息（带 ACK 模式）
   *
   * @param topic Topic 名称
   * @param key 消息键
   * @param value 消息值
   * @param ack ACK 模式
   * @param result 可选的输出参数，成功时写入生产结果
   * @return true 生产成功；false 发送失败
   */
  bool produce(const std::string& topic, const std::string& key, const std::string& value,
               AckMode ack, ProduceResult* result = nullptr);

  /**
   * @brief 批量生产消息
   *
   * @param topic Topic 名称
   * @param messages 消息列表
   * @param ack ACK 模式
   * @param results 可选的输出参数，成功时写入每条消息的生产结果
   * @return true 所有消息生产成功；false 部分或全部失败
   */
  bool produceBatch(const std::string& topic, std::vector<ProducerMessage> messages,
                    AckMode ack = AckMode::kOne, std::vector<ProduceResult>* results = nullptr);

  /**
   * @brief 刷盘
   *
   * 强制将所有未持久化的数据写入磁盘。
   *
   * @return true 刷盘成功；false IO 错误
   */
  bool flush();

  /**
   * @brief 列出所有 Topic
   *
   * @param topics 输出参数，Topic 列表
   * @return true 查询成功；false 网络错误
   */
  bool listTopics(std::vector<TopicInfo>* topics);

  /**
   * @brief 获取系统指标
   *
   * @param output 输出参数，JSON 格式的指标数据
   * @return true 查询成功；false 网络错误
   */
  bool metrics(std::string* output);

  /**
   * @brief 关闭连接
   *
   * 必须在析构前调用，释放网络资源。
   */
  void close();

  /**
   * @brief 设置超时时间
   *
   * @param timeout_ms 超时时间（毫秒）
   */
  void setTimeoutMs(std::uint32_t timeout_ms);

  /**
   * @brief 设置认证 Token
   *
   * @param token 认证 Token
   */
  void setAuthToken(std::string token);

  /**
   * @brief 设置 TLS 配置
   *
   * @param options TLS 配置选项
   */
  void setTlsOptions(network::TlsOptions options);

  /**
   * @brief 设置 Producer ID
   *
   * 用于幂等生产和消息去重。
   *
   * @param producer_id Producer 唯一标识
   */
  void setProducerId(std::uint64_t producer_id);

  /**
   * @brief 获取最近的错误信息
   *
   * @return 错误信息字符串
   */
  const std::string& lastError() const;

 public:
  /**
   * @brief 内部实现结构体
   * 使用 Pimpl 模式隐藏实现细节
   */
  struct Impl;

 private:
  /**
   * @brief 内部实现指针
   */
  std::unique_ptr<Impl> impl_;
};

/**
 * @brief 消息消费者客户端
 *
 * 提供消息消费的高层 API，支持消费者组和分区分配。
 *
 * 功能：
 * - 连接 Broker
 * - 订阅 Topic
 * - 加入消费者组
 * - 拉取消息
 * - 提交消费偏移量
 *
 * 设计要点：
 * 1. 封装协议帧：调用方只需处理消息内容
 * 2. 自动重连：网络断开时自动重连
 * 3. 消费者组：支持自动分区分配
 * 4. 错误处理：通过 lastError 获取错误详情
 *
 * 使用示例：
 * @code
 * MqConsumer consumer;
 * consumer.connect("localhost", 9000);
 * consumer.subscribe("my-topic", "my-group");
 *
 * while (true) {
 *   auto msg = consumer.poll(5000);
 *   if (msg) {
 *     std::cout << "收到消息: " << msg->value << std::endl;
 *     consumer.commit(msg->offset + 1);
 *   }
 * }
 * @endcode
 *
 * 消费者组协议：
 * 1. joinGroup：加入组，获取成员 ID
 * 2. syncGroup：同步分区分配
 * 3. poll：拉取消息
 * 4. commit：提交偏移量
 * 5. heartbeat：定期发送心跳（自动）
 *
 * 线程安全性：
 * - 单个实例不能并发使用
 * - 多个实例可以并发使用
 */
class MqConsumer {
 public:
  /**
   * @brief 构造函数
   */
  MqConsumer();

  /**
   * @brief 析构函数，自动关闭连接
   */
  ~MqConsumer();

  // 禁用拷贝
  MqConsumer(const MqConsumer&) = delete;

  /**
   * @brief 连接到单个 Broker
   *
   * @param host Broker 地址
   * @param port Broker 端口
   * @return true 连接成功；false 连接失败
   */
  bool connect(const std::string& host, std::uint16_t port);

  /**
   * @brief 连接到多个 Broker（自动选择）
   *
   * @param endpoints Broker 地址列表
   * @return true 连接成功；false 所有 Broker 都不可用
   */
  bool connect(const std::vector<std::pair<std::string, std::uint16_t>>& endpoints);

  /**
   * @brief 订阅 Topic
   *
   * @param topic Topic 名称
   * @param group 消费者组名称
   * @return true 订阅成功；false 网络错误
   */
  bool subscribe(const std::string& topic, const std::string& group);

  /**
   * @brief 订阅指定分区
   *
   * @param topic Topic 名称
   * @param group 消费者组名称
   * @param partition 分区号
   * @return true 订阅成功；false 网络错误
   */
  bool subscribe(const std::string& topic, const std::string& group, std::uint32_t partition);

  /**
   * @brief 加入消费者组
   *
   * 加入流程：
   * 1. 发送 JoinGroup 请求
   * 2. 获取成员 ID
   * 3. 等待分区分配
   *
   * @param group 消费者组名称
   * @param member_id 消费者成员 ID
   * @param topics 订阅的 Topic 列表
   * @return true 加入成功；false 网络错误
   */
  bool joinGroup(const std::string& group, const std::string& member_id,
                 const std::vector<std::string>& topics);

  /**
   * @brief 同步消费者组分配
   *
   * 同步流程：
   * 1. 发送 SyncGroup 请求
   * 2. 获取分区分配结果
   *
   * @param assignments 输出参数，分配的分区列表
   * @return true 同步成功；false 网络错误
   */
  bool syncGroup(std::vector<core::GroupAssignment>* assignments);

  /**
   * @brief 获取消费偏移量
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param offset 输出参数，消费偏移量
   * @return true 获取成功；false 网络错误
   */
  bool fetchGroupOffsets(const std::string& topic, std::uint32_t partition,
                         std::uint64_t* offset);

  /**
   * @brief 拉取消息
   *
   * 阻塞等待直到有消息或超时。
   *
   * @param timeout_ms 超时时间（毫秒），默认 5000ms
   * @return 成功返回消息，超时返回 std::nullopt
   */
  std::optional<core::Message> poll(std::uint32_t timeout_ms = 5000);

  /**
   * @brief 提交消费偏移量
   *
   * 提交后，下次重启会从该位置继续消费。
   *
   * @param offset 要提交的偏移量
   * @return true 提交成功；false 网络错误
   */
  bool commit(std::uint64_t offset);

  /**
   * @brief 关闭连接
   *
   * 必须在析构前调用，释放网络资源。
   */
  void close();

  /**
   * @brief 设置超时时间
   *
   * @param timeout_ms 超时时间（毫秒）
   */
  void setTimeoutMs(std::uint32_t timeout_ms);

  /**
   * @brief 设置认证 Token
   *
   * @param token 认证 Token
   */
  void setAuthToken(std::string token);

  /**
   * @brief 设置 TLS 配置
   *
   * @param options TLS 配置选项
   */
  void setTlsOptions(network::TlsOptions options);

  /**
   * @brief 获取最近的错误信息
   *
   * @return 错误信息字符串
   */
  const std::string& lastError() const;

 public:
  /**
   * @brief 内部实现结构体
   */
  struct Impl;

 private:
  /**
   * @brief 内部实现指针
   */
  std::unique_ptr<Impl> impl_;
};

}  // namespace mq::client