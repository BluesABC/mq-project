#pragma once

#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mq::core {

/**
 * @brief fsync 刷盘策略枚举
 *
 * 控制 WAL（Write-Ahead Log）的持久化策略，在可靠性和吞吐量之间权衡：
 * - kPerMessage：每条消息写入后立即 fsync，最高可靠性但性能最低
 * - kPerBatch：每批次写入后 fsync，平衡可靠性和性能（默认）
 * - kInterval：按时间间隔定期 fsync，最高性能但可能丢失少量数据
 *
 * 配置为 kPerBatch 或 kInterval 时，需要显式调用 Flush 确保数据持久化。
 */
enum class FsyncPolicy { kPerMessage, kPerBatch, kInterval };

/**
 * @brief 存储引擎配置结构体
 *
 * 包含所有可配置的存储参数，每个参数都有合理的默认值。
 * 生产环境应根据硬件特性和业务需求调整这些参数。
 */
struct StorageConfig {
  /**
   * @brief 段文件大小上限（字节）
   * 默认 64MB，单个段文件达到此大小后会滚动创建新段。
   * 较大的段文件减少文件数量但增加恢复时间，64MB 是经验值。
   */
  std::uint64_t segment_size_bytes = 64ULL * 1024 * 1024;

  /**
   * @brief 索引间隔
   * 每写入 N 条消息创建一个索引条目，用于快速定位 offset。
   * 较大的值节省索引空间但降低查找精度，1000 是推荐值。
   */
  std::uint32_t index_interval = 1000;

  /**
   * @brief fsync 策略
   * 默认 kPerBatch 在可靠性和性能之间取得平衡。
   */
  FsyncPolicy fsync_policy = FsyncPolicy::kPerBatch;

  /**
   * @brief fsync 间隔（毫秒）
   * 仅当 fsync_policy == kInterval 时生效，控制定期刷盘频率。
   * 5ms 是推荐值，兼顾延迟和吞吐。
   */
  std::uint32_t fsync_interval_ms = 5;

  /**
   * @brief 消息保留时间（毫秒）
   * 默认 7 天，超过此时间的消息会被清理线程删除。
   * 适用于按时间清理的场景，如日志类消息。
   */
  std::uint64_t retention_ms = 7ULL * 24 * 60 * 60 * 1000;

  /**
   * @brief 消息保留大小上限（字节）
   * 默认 1GB，单个分区的消息总量超过此值时触发清理。
   * 与 retention_ms 取并集，任一条件满足即清理。
   */
  std::uint64_t retention_bytes = 1024ULL * 1024 * 1024;

  /**
   * @brief 清理线程检查间隔（毫秒）
   * 控制清理线程扫描过期段文件的频率。
   * 1000ms 是推荐值，避免过于频繁的 IO 操作。
   */
  std::uint32_t cleaner_interval_ms = 1000;
};

/**
 * @brief 消息结构体
 *
 * 表示存储引擎中的一条消息，是 Producer 发送和 Consumer 接收的基本单元。
 * offset 在同一 Topic 分区内唯一且递增，但不同分区的 offset 不能比较。
 */
struct Message {
  /**
   * @brief 消息偏移量
   * 在当前分区内的唯一标识，从 0 开始递增。
   * 不能跨分区比较或提交，仅在同一分区内有效。
   */
  std::uint64_t offset = 0;

  /**
   * @brief 消息时间戳（毫秒）
   * 消息创建或写入的时间，用于消息保留策略和顺序保证。
   */
  std::int64_t timestamp_ms = 0;

  /**
   * @brief 消息键
   * 可选的业务键，用于自动分区时的哈希路由，相同 Key 的消息会落在同一分区。
   */
  std::string key;

  /**
   * @brief 消息值（负载）
   * 消息的实际内容，由业务层定义格式。
   */
  std::string value;
};

/**
 * @brief 存储引擎
 *
 * 负责消息的持久化存储，是消息队列的核心组件。
 *
 * 在整体架构中的角色：
 * - 接收 Broker 层的写入/读取请求
 * - 管理 WAL（Write-Ahead Log）和索引文件
 * - 处理段文件滚动、过期清理、数据恢复
 *
 * 与其他模块的关系：
 * - QueueManager 负责元数据和分区路由，本类负责实际数据存储
 * - Network 层通过 Broker 间接调用本类，不直接访问
 * - 本类不依赖协议层，只处理 Message 结构体
 *
 * 设计要点：
 * - 所有写入必须先经过 WAL，保证数据持久性
 * - 使用内存映射（mmap）加速读取
 * - 后台清理线程负责过期段文件删除
 */
class StorageEngine {
 public:
  /**
   * @brief 构造函数，使用默认配置
   * @param data_dir 数据存储目录，不存在时会自动创建
   */
  explicit StorageEngine(std::filesystem::path data_dir);

  /**
   * @brief 构造函数，使用自定义配置
   * @param data_dir 数据存储目录
   * @param config 存储配置参数
   */
  StorageEngine(std::filesystem::path data_dir, StorageConfig config);

  /**
   * @brief 析构函数，停止清理线程并释放资源
   */
  ~StorageEngine();

  // 禁用拷贝，防止资源重复释放
  StorageEngine(const StorageEngine&) = delete;
  StorageEngine& operator=(const StorageEngine&) = delete;

  /**
   * @brief 打开存储引擎，建立目录结构并启动后台清理线程
   *
   * 这是使用存储引擎的第一步，必须在 Append/Read 之前调用。
   * 后续所有写入都必须通过本接口进入 WAL。
   *
   * @param error 可选的错误信息输出参数
   * @return true 初始化成功；false 目录创建失败或已打开
   */
  bool Open(std::string* error = nullptr);

  /**
   * @brief 追加消息到指定分区
   *
   * 写入流程：
   * 1. 写入 WAL（保证持久性）
   * 2. 更新内存索引（加速读取）
   * 3. 根据 fsync 策略决定是否立即刷盘
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param key 消息键，用于自动分区路由
   * @param value 消息值
   * @param message 输出参数，成功时写入完整的消息信息（含 offset 和 timestamp）
   * @param error 可选的错误信息输出参数
   * @return true 写入成功；false 存储错误或 Topic 不存在
   */
  bool Append(const std::string& topic, std::uint32_t partition, std::string key, std::string value,
              Message* message, std::string* error = nullptr);

  /**
   * @brief 副本追加，要求 offset 连续
   *
   * 用于主从复制场景，Follower 节点接收 Leader 的日志。
   * 要求 offset 连续是为了发现复制缺口（数据丢失或乱序），
   * 而不是静默覆盖数据导致数据不一致。
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param message 要追加的消息（必须携带正确的 offset）
   * @param error 可选的错误信息输出参数
   * @return true 追加成功；false offset 不连续或存储错误
   */
  bool AppendReplica(const std::string& topic, std::uint32_t partition, const Message& message,
                     std::string* error = nullptr);

  /**
   * @brief 删除整个 Topic 的所有分区数据
   *
   * @param topic Topic 名称
   * @param error 可选的错误信息输出参数
   * @return true 删除成功；false Topic 不存在或删除失败
   */
  bool DeleteTopic(const std::string& topic, std::string* error = nullptr);

  /**
   * @brief 从指定分区读取消息
   *
   * 读取流程：
   * 1. 从索引定位到大致位置
   * 2. 从 WAL 读取实际消息
   * 3. 返回不超过 max_bytes 的消息列表
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param start_offset 起始 offset（inclusive）
   * @param max_bytes 单次返回的最大字节数，防止无界内存增长
   * @param messages 输出参数，读取到的消息列表
   * @param error 可选的错误信息输出参数
   * @return true 读取成功（可能返回空列表表示已读完）；false 存储错误
   */
  bool Read(const std::string& topic, std::uint32_t partition, std::uint64_t start_offset,
            std::uint32_t max_bytes, std::vector<Message>* messages,
            std::string* error = nullptr) const;

  /**
   * @brief 获取指定分区的下一个可写 offset
   *
   * 用于 Producer 写入前确定 offset，或 Consumer 了解消费进度。
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param offset 输出参数，下一个可写 offset
   * @param error 可选的错误信息输出参数
   * @return true 查询成功；false 分区不存在
   */
  bool NextOffset(const std::string& topic, std::uint32_t partition, std::uint64_t* offset,
                  std::string* error = nullptr) const;

  /**
   * @brief 强制刷盘，将所有未持久化的数据写入磁盘
   *
   * 在 kPerBatch 或 kInterval 策略下，调用此方法确保数据持久化。
   * 在关闭存储引擎前应调用此方法，避免数据丢失。
   *
   * @param error 可选的错误信息输出参数
   * @return true 刷盘成功；false IO 错误
   */
  bool Flush(std::string* error = nullptr);

  /**
   * @brief 手动触发过期段文件清理
   *
   * 通常由后台清理线程自动执行，此方法用于手动触发（如测试或运维场景）。
   *
   * @param error 可选的错误信息输出参数
   * @return true 清理成功；false 删除文件失败
   */
  bool CleanupExpiredSegments(std::string* error = nullptr);

 private:
  /**
   * @brief 分区内部结构
   * 每个 Topic-Partition 组合对应一个 Partition 实例，
   * 管理该分区的所有段文件、索引和元数据。
   */
  struct Partition;

  /**
   * @brief 获取指定分区的 Partition 对象
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param error 可选的错误信息输出参数
   * @return Partition 指针，不存在时返回 nullptr
   */
  Partition* GetPartition(const std::string& topic, std::uint32_t partition,
                          std::string* error) const;

  /**
   * @brief 恢复指定分区的数据
   *
   * 在 Open 时调用，回放 WAL 中未提交的数据，保证数据一致性。
   *
   * @param partition 要恢复的分区
   * @param error 可选的错误信息输出参数
   * @return true 恢复成功；false 数据损坏或 IO 错误
   */
  bool Recover(Partition* partition, std::string* error) const;

  /**
   * @brief 后台清理线程主函数
   *
   * 周期性扫描所有分区，删除满足过期条件的段文件。
   * 使用条件变量等待，支持优雅退出。
   */
  void CleanerLoop();

  std::filesystem::path data_dir_;  ///< 数据存储目录
  StorageConfig config_;            ///< 存储配置

  /**
   * @brief 互斥锁，保护 partitions_ 的并发访问
   * 使用 mutable 允许在 const 方法（如 Read）中加锁
   */
  mutable std::mutex mutex_;

  /**
   * @brief 所有分区的实例列表
   * 使用 unique_ptr 管理生命周期，避免内存泄漏
   */
  mutable std::vector<std::unique_ptr<Partition>> partitions_;

  std::thread cleaner_thread_;          ///< 后台清理线程
  std::condition_variable cleaner_cv_;  ///< 清理线程的等待条件变量
  bool stop_cleaner_ = false;           ///< 退出标志，通知清理线程停止
  bool opened_ = false;                 ///< 是否已打开，防止重复打开
};

}  // namespace mq::core