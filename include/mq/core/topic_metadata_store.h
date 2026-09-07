#pragma once

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "mq/core/queue_manager.h"

namespace mq::core {

/**
 * @brief Topic 元数据存储
 *
 * 负责 Topic 元数据的持久化存储和恢复。
 *
 * 在整体架构中的角色：
 * - Broker 处理 CreateTopic/DeleteTopic 请求时调用 Save
 * - Broker 启动时调用 Load 恢复所有 Topic 配置
 * - QueueManager 依赖本类进行元数据持久化
 *
 * 与其他模块的关系：
 * - QueueManager 负责元数据的内存管理，本类负责持久化
 * - StorageEngine 不直接访问本类，通过 QueueManager 间接使用
 *
 * 存储格式：
 * - 使用简单的二进制格式，每个 Topic 元数据固定大小
 * - 文件按时间戳命名，支持版本管理
 *
 * 设计要点：
 * - 保存是原子操作，不会出现部分写入
 * - 加载时会校验数据完整性
 * - 支持增量保存和全量保存两种模式
 *
 * 注意事项：
 * - 本类只负责存储，不负责内存中的元数据管理
 * - 修改 Topic 配置时应先更新 QueueManager，再调用 Save
 */
class TopicMetadataStore {
 public:
  /**
   * @brief 构造函数
   *
   * @param path 元数据存储文件路径
   */
  explicit TopicMetadataStore(std::filesystem::path path) : path_(std::move(path)) {}

  /**
   * @brief 保存所有 Topic 元数据
   *
   * 保存流程：
   * 1. 将所有 Topic 元数据序列化为二进制格式
   * 2. 写入临时文件
   * 3. 原子重命名临时文件为目标文件
   *
   * @param topics 要保存的 Topic 元数据列表
   * @param error 可选的错误信息输出参数
   * @return true 保存成功；false IO 错误
   */
  bool Save(const std::vector<TopicMetadata>& topics, std::string* error = nullptr) const;

  /**
   * @brief 加载所有 Topic 元数据
   *
   * 加载流程：
   * 1. 读取文件内容
   * 2. 校验数据完整性
   * 3. 反序列化为 TopicMetadata 列表
   *
   * @param topics 输出参数，加载到的 Topic 元数据列表
   * @param error 可选的错误信息输出参数
   * @return true 加载成功（可能返回空列表）；false 文件损坏或 IO 错误
   */
  bool Load(std::vector<TopicMetadata>* topics, std::string* error = nullptr) const;

 private:
  std::filesystem::path path_;  ///< 存储文件路径
};

}  // namespace mq::core