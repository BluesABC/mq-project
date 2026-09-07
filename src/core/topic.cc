#include "mq/core/topic.h"

#include <cstddef>

namespace mq::core {
namespace {

/**
 * @brief 检查是否是 UTF-8 续字节
 *
 * UTF-8 编码规则：
 * - 单字节字符：0xxxxxxx（0x00-0x7F）
 * - 多字节首字节：11xxxxxx
 * - 续字节：10xxxxxx（0x80-0xBF）
 *
 * @param byte 待检查的字节
 * @return true 是续字节；false 不是
 */
bool IsContinuation(unsigned char byte) {
  return (byte & 0xC0) == 0x80;
}

/**
 * @brief 校验字符串是否是有效的 UTF-8 编码
 *
 * UTF-8 编码规则：
 * - 1 字节：0xxxxxxx（ASCII）
 * - 2 字节：110xxxxx 10xxxxxx
 * - 3 字节：1110xxxx 10xxxxxx 10xxxxxx
 * - 4 字节：11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
 *
 * 无效编码检测：
 * - 非法的首字节（如 0x80-0xBF 单独出现）
 * - 续字节数量不足
 * - 超出 Unicode 范围（如 0xF4 以上）
 * - 无效的代理对（0xED 0xA0-0xBF）
 *
 * @param value 待校验的字符串
 * @return true 有效的 UTF-8；false 无效的 UTF-8
 */
bool IsValidUtf8(std::string_view value) {
  for (std::size_t index = 0; index < value.size();) {
    const unsigned char first = static_cast<unsigned char>(value[index]);

    // 单字节字符（ASCII）
    if (first < 0x80) {
      ++index;
      continue;
    }

    // 确定多字节字符的长度
    std::size_t length = 0;
    if (first >= 0xC2 && first <= 0xDF) {
      length = 2;  // 2 字节
    } else if (first >= 0xE0 && first <= 0xEF) {
      length = 3;  // 3 字节
    } else if (first >= 0xF0 && first <= 0xF4) {
      length = 4;  // 4 字节
    } else {
      return false;  // 非法的首字节
    }

    // 检查剩余长度
    if (index + length > value.size()) return false;

    // 检查续字节
    for (std::size_t offset = 1; offset < length; ++offset) {
      if (!IsContinuation(static_cast<unsigned char>(value[index + offset]))) return false;
    }

    // 检查无效的编码范围
    const unsigned char second = static_cast<unsigned char>(value[index + 1]);
    if ((first == 0xE0 && second < 0xA0) || (first == 0xED && second >= 0xA0) ||
        (first == 0xF0 && second < 0x90) || (first == 0xF4 && second >= 0x90)) {
      return false;
    }

    index += length;
  }
  return true;
}

}  // namespace

/**
 * @brief 校验 Topic 名称是否合法
 *
 * Topic 名称规范：
 * - 长度：1-64 字符
 * - 字符：可打印 ASCII 字符，排除特殊字符
 * - 编码：必须是有效的 UTF-8 序列
 * - 保留名：不能是 "." 或 ".."
 *
 * 排除的特殊字符：
 * - / \ : * ? " < > |（文件系统保留字符）
 * - 控制字符（< 0x20）
 *
 * @param topic 要校验的 Topic 名称
 * @return true 名称合法；false 名称非法
 */
bool IsValidTopicName(std::string_view topic) {
  // 长度校验和保留名检查
  if (topic.empty() || topic.size() > 64 || topic == "." || topic == "..") return false;

  // 字符校验
  for (const unsigned char byte : topic) {
    if (byte < 0x20 || byte == '/' || byte == '\\' || byte == ':' || byte == '*' || byte == '?' ||
        byte == '"' || byte == '<' || byte == '>' || byte == '|') {
      return false;
    }
  }

  // UTF-8 编码校验
  return IsValidUtf8(topic);
}

}  // namespace mq::core