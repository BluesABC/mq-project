#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "mq/protocol/commands.h"

namespace mq::protocol {

/**
 * @brief 协议编解码器
 *
 * 负责协议帧的边界校验和编解码。
 * 网络层无需了解字段的二进制布局，只负责传输字节流。
 *
 * 设计要点：
 * 1. 静态方法：无状态，线程安全
 * 2. 边界校验：解码时检查帧完整性
 * 3. 错误处理：返回详细错误信息
 *
 * 在整体架构中的角色：
 * - TcpConnection 调用本类解码接收到的字节
 * - Broker 调用本类编码响应
 * - 本类是协议层的核心，隔离了网络层和业务层
 *
 * 与其他模块的关系：
 * - TcpConnection 使用 RequestStreamDecoder 处理粘包
 * - Broker 使用 EncodeResponse 编码响应
 * - commands.h 定义了协议的数据结构
 *
 * 帧格式：
 * 请求帧：
 * ```
 * [Magic(2)][Version(1)][Command(1)][RequestId(8)]
 * [Flags(2)][TopicLen(2)][Topic(N)]
 * [PayloadLen(4)][Payload(M)]
 * ```
 * 响应帧：
 * ```
 * [Magic(2)][Version(1)][Status(1)][RequestId(8)][Flags(2)][PayloadLen(4)][Payload(M)]
 * ```
 *
 * 注意事项：
 * - 编码失败时会清空输出 buffer
 * - 解码失败时会设置 error 信息
 * - 所有整数字段使用网络字节序（大端）
 */
class ProtocolCodec {
 public:
  /**
   * @brief 编码请求为协议帧
   *
   * 编码流程：
   * 1. 校验字段合法性
   * 2. 按照帧格式序列化
   * 3. 写入输出 buffer
   *
   * @param request 要编码的请求
   * @param frame 输出参数，编码后的字节流
   * @param error 可选的错误信息输出参数
   * @return true 编码成功；false 字段非法或 buffer 错误
   */
  static bool EncodeRequest(const Request& request, std::string* frame,
                            std::string* error = nullptr);

  /**
   * @brief 编码响应为协议帧
   *
   * @param response 要编码的响应
   * @param frame 输出参数，编码后的字节流
   * @param error 可选的错误信息输出参数
   * @return true 编码成功；false 字段非法或 buffer 错误
   */
  static bool EncodeResponse(const Response& response, std::string* frame,
                             std::string* error = nullptr);

  /**
   * @brief 解码协议帧为请求
   *
   * 解码流程：
   * 1. 校验魔数和版本
   * 2. 解析字段长度
   * 3. 反序列化为 Request 结构体
   *
   * @param frame 要解码的字节流
   * @param request 输出参数，解码后的请求
   * @param error 可选的错误信息输出参数
   * @return true 解码成功；false 帧格式错误
   */
  static bool DecodeRequest(std::string_view frame, Request* request, std::string* error = nullptr);

  /**
   * @brief 解码协议帧为响应
   *
   * @param frame 要解码的字节流
   * @param response 输出参数，解码后的响应
   * @param error 可选的错误信息输出参数
   * @return true 解码成功；false 帧格式错误
   */
  static bool DecodeResponse(std::string_view frame, Response* response,
                             std::string* error = nullptr);
};

/**
 * @brief 请求流解码器
 *
 * 处理 TCP 粘包/半包问题，将字节流转换为 Request 对象。
 *
 * 设计要点：
 * 1. 内部缓冲：累积接收到的数据
 * 2. 帧边界检测：根据协议帧格式判断帧完整性
 * 3. 多帧处理：一次 Push 可能解码出多个帧
 *
 * 使用场景：
 * - TcpConnection 的 OnReadable 方法
 * - 处理 TCP 的流式数据
 *
 * 使用示例：
 * @code
 * RequestStreamDecoder decoder;
 * std::vector<Request> requests;
 * std::string error;
 *
 * // 接收到数据时调用
 * if (!decoder.Push(data, &requests, &error)) {
 *   // 处理错误
 * }
 *
 * // 处理解码出的请求
 * for (const auto& req : requests) {
 *   // ...
 * }
 * @endcode
 *
 * 注意事项：
 * - 半帧数据会被缓存，等待后续数据
 * - 缓冲区无限增长可能导致 OOM，需要外部限制
 * - 解码失败时返回错误，调用方应关闭连接
 */
class RequestStreamDecoder {
 public:
  /**
   * @brief 推送数据到解码器
   *
   * 推送流程：
   * 1. 将数据追加到内部缓冲区
   * 2. 尝试解码完整的帧
   * 3. 解码成功的帧加入输出列表
   * 4. 剩余的半帧保留在缓冲区
   *
   * @param bytes 接收到的原始数据
   * @param requests 输出参数，解码出的请求列表
   * @param error 可选的错误信息输出参数
   * @return true 处理成功（可能解码出 0 个或多个请求）；false 协议错误
   */
  bool Push(std::string_view bytes, std::vector<Request>* requests, std::string* error = nullptr);

 private:
  /**
   * @brief 内部缓冲区
   * 累积未解码的数据
   */
  std::string buffer_;
};

}  // namespace mq::protocol