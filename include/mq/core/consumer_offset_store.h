#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace mq::core {

/**
 * @brief 消费者偏移量记录
 *
 * 记录消费者组在某个 Topic-Partition 上的消费进度。
 * 以 group/topic/partition 为键持久化，重启后可恢复消费进度。
 *
 * 设计要点：
 * - offset 表示下一条要消费的消息位置
 * - 提交 offset 是原子操作，要么全部成功，要么全部失败
 * - 支持自动提交和手动提交两种模式
 */
struct ConsumerOffset {
  std::string group;            ///< 消费者组名称
  std::string topic;            ///< Topic 名称
  std::uint32_t partition = 0;  ///< 分区号
  std::uint64_t offset = 0;     ///< 消费偏移量（下一条要消费的位置）
};

/**
 * @brief 消费者偏移量存储
 *
 * 负责消费者偏移量的持久化存储和恢复。
 *
 * 在整体架构中的角色：
 * - Broker 处理 Consumer 的 CommitOffset 请求时调用 Save
 * - Broker 启动时调用 Load 恢复所有消费者的偏移量
 * - Consumer 启动时通过 OffsetFetch 获取上次提交的偏移量
 *
 * 与其他模块的关系：
 * - 依赖 ConsumerGroupCoordinator 获取消费者组信息
 * - 依赖 StorageEngine 读取消息时需要偏移量
 *
 * 存储格式：
 * - 使用简单的二进制格式，每个偏移量记录固定大小
 * - 文件按 group 分组存储，便于快速查询和恢复
 *
 * 注意事项：
 * - 保存是原子操作，不会出现部分写入
 * - 加载时会校验数据完整性，损坏的数据会被跳过
 */
class ConsumerOffsetStore {
 public:
  /**
   * @brief 构造函数
   *
   * @param path 偏移量存储文件路径
   */
  explicit ConsumerOffsetStore(std::filesystem::path path) : path_(std::move(path)) {}

  /**
   * @brief 批量保存消费者偏移量
   *
   * 保存流程：
   * 1. 将所有偏移量序列化为二进制格式
   * 2. 写入临时文件
   * 3. 原子重命名临时文件为目标文件
   *
   * @param offsets 要保存的偏移量列表
   * @param error 可选的错误信息输出参数
   * @return true 保存成功；false IO 错误
   */
  bool Save(const std::vector<ConsumerOffset>& offsets, std::string* error = nullptr) const;

  /**
   * @brief 加载所有消费者偏移量
   *
   * 加载流程：
   * 1. 读取文件内容
   * 2. 校验数据完整性
   * 3. 反序列化为 ConsumerOffset 列表
   *
   * @param offsets 输出参数，加载到的偏移量列表
   * @param error 可选的错误信息输出参数
   * @return true 加载成功（可能返回空列表）；false 文件损坏或 IO 错误
   */
  bool Load(std::vector<ConsumerOffset>* offsets, std::string* error = nullptr) const;

 private:
  std::filesystem::path path_;  ///< 存储文件路径
};

}  // namespace mq::core