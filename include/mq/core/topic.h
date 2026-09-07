#pragma once

#include <string_view>

namespace mq::core {

/**
 * @brief 校验 Topic 名称是否合法
 *
 * Topic 名称规范：
 * - 长度：1-64 字符
 * - 字符：可打印 ASCII 字符，排除特殊字符（/ \ : * ? " < > |）
 * - 编码：必须是有效的 UTF-8 序列
 * - 保留名：不能是 "." 或 ".."
 *
 * 使用场景：
 * - 创建 Topic 时校验名称
 * - 生产/消费消息前校验 Topic 名称
 * - 协议解析时校验请求参数
 *
 * @param topic 要校验的 Topic 名称
 * @return true 名称合法；false 名称非法
 *
 * 示例：
 * @code
 * if (!IsValidTopicName(request.topic)) {
 *   return Response{Status::kBadRequest};
 * }
 * @endcode
 */
bool IsValidTopicName(std::string_view topic);

}  // namespace mq::core