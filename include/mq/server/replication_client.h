#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mq/core/storage_engine.h"

namespace mq::server {

/**
 * @brief 复制客户端
 *
 * 与远端 Broker 进行复制通信的客户端。
 *
 * 设计要点：
 * 1. 简单封装：只负责发送请求和接收响应
 * 2. 错误处理：通过返回值和 lastError 交给协调层处理
 * 3. 超时控制：支持配置请求超时
 *
 * 在整体架构中的角色：
 * - ReplicationCoordinator 调用本类执行实际的网络通信
 * - 本类不处理复制逻辑，只负责消息传递
 *
 * 与其他模块的关系：
 * - ReplicationCoordinator 根据本类的返回结果更新状态
 * - 本类与远端 Broker 的 TcpServer 通信
 *
 * 协议格式：
 * - 使用标准的协议帧格式
 * - 复制请求携带 kFlagReplication 标志
 * - 心跳请求定期发送，检测节点存活
 *
 * 线程安全性：
 * - 单个实例不能并发使用
 * - 多个实例可以并发使用
 *
 * 错误处理：
 * - 网络错误通过 lastError() 获取
 * - 协议错误返回 false
 */
class ReplicationClient {
 public:
  /**
   * @brief 构造函数
   *
   * @param host 远端 Broker 地址
   * @param port 远端 Broker 端口
   * @param timeout_ms 请求超时（毫秒），默认 1000ms
   * @param auth_token 认证 Token（可选）
   */
  ReplicationClient(std::string host, std::uint16_t port, std::uint32_t timeout_ms = 1000,
                    std::string auth_token = {});

  /**
   * @brief 从远端拉取日志
   *
   * Follower 调用此方法从 Leader 拉取日志。
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param offset 起始 offset
   * @param max_bytes 最大拉取字节数
   * @param messages 输出参数，拉取到的消息列表
   * @return true 拉取成功；false 网络错误或超时
   */
  bool Fetch(const std::string& topic, std::uint32_t partition, std::uint64_t offset,
             std::uint32_t max_bytes, std::vector<core::Message>* messages);

  /**
   * @brief 追加日志到远端
   *
   * Leader 调用此方法将日志复制到 Follower。
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param messages 要追加的消息列表
   * @param term 任期
   * @param commit_index Leader 的提交索引
   * @param leader_id Leader ID
   * @param prev_log_index 前一条日志的索引（用于一致性检查）
   * @param prev_log_term 前一条日志的任期
   * @param next_log_index 输出参数，下一个日志索引
   * @return true 追加成功；false 任期不匹配或网络错误
   */
  bool Append(const std::string& topic, std::uint32_t partition,
              const std::vector<core::Message>& messages, std::uint64_t term = 0,
              std::uint64_t commit_index = 0, const std::string& leader_id = {},
              std::uint64_t prev_log_index = 0, std::uint64_t prev_log_term = 0,
              std::uint64_t* next_log_index = nullptr);

  /**
   * @brief 发送心跳
   *
   * 定期调用以保持 Leader 状态和同步进度。
   *
   * @param topic Topic 名称
   * @param partition 分区号
   * @param node_id 发送者节点 ID
   * @param replicated_offset 已复制的 offset
   * @param term 任期（Leader 调用时使用）
   * @param commit_index 提交索引（Leader 调用时使用）
   * @return true 心跳成功；false 网络错误
   */
  bool Heartbeat(const std::string& topic, std::uint32_t partition, const std::string& node_id,
                 std::uint64_t replicated_offset, std::uint64_t term = 0,
                 std::uint64_t commit_index = 0);

  /**
   * @brief 发送心跳（旧接口，兼容单分区场景）
   *
   * 保留旧调用形式，默认使用无主题的兼容心跳，
   * 仅供已有单分区调用方迁移。
   *
   * @param node_id 发送者节点 ID
   * @param replicated_offset 已复制的 offset
   * @return true 心跳成功；false 网络错误
   */
  bool Heartbeat(const std::string& node_id, std::uint64_t replicated_offset) {
    return Heartbeat("__legacy__", 0, node_id, replicated_offset);
  }

  /**
   * @brief 请求投票
   *
   * 候选者调用此方法请求其他节点投票。
   *
   * @param term 请求者的任期
   * @param candidate_id 候选者 ID
   * @param granted 输出参数，是否授予投票
   * @param last_log_index 最后日志索引（可选）
   * @param last_log_term 最后日志任期（可选）
   * @return true 请求成功；false 网络错误
   */
  bool Vote(std::uint64_t term, const std::string& candidate_id, bool* granted,
            std::uint64_t last_log_index = 0, std::uint64_t last_log_term = 0);

  /**
   * @brief 请求预投票
   *
   * PreVote 机制：在正式选举前先收集预投票，
   * 如果预投票失败则不增加任期。
   *
   * @param term 预投票任期
   * @param candidate_id 候选者 ID
   * @param granted 输出参数，是否授予预投票
   * @param last_log_index 最后日志索引（可选）
   * @param last_log_term 最后日志任期（可选）
   * @return true 请求成功；false 网络错误
   */
  bool PreVote(std::uint64_t term, const std::string& candidate_id, bool* granted,
               std::uint64_t last_log_index = 0, std::uint64_t last_log_term = 0);

  /**
   * @brief 获取最近的错误信息
   * @return 错误信息字符串
   */
  const std::string& lastError() const {
    return error_;
  }

 private:
  /**
   * @brief 发送请求并接收响应
   *
   * @param command 命令类型
   * @param topic Topic 名称
   * @param payload 请求载荷
   * @param response_payload 输出参数，响应载荷
   * @param term_payload 是否包含任期信息
   * @return true 请求成功；false 网络错误或超时
   */
  bool Call(std::uint8_t command, const std::string& topic, std::string payload,
            std::string* response_payload, bool term_payload = false);

  // ==================== 成员变量 ====================

  std::string host_;              ///< 远端 Broker 地址
  std::uint16_t port_;            ///< 远端 Broker 端口
  std::uint32_t timeout_ms_;      ///< 请求超时（毫秒）
  std::string auth_token_;        ///< 认证 Token
  std::uint64_t request_id_ = 1;  ///< 请求 ID 计数器
  std::string error_;             ///< 最近的错误信息
};

}  // namespace mq::server