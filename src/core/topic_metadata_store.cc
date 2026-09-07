#include "mq/core/topic_metadata_store.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <utility>

#include "mq/core/topic.h"

#ifdef _WIN32
#include <windows.h>
#endif

namespace mq::core {
namespace {

/**
 * @brief 文件格式版本号
 *
 * 用于向后兼容，当格式变化时递增版本号。
 */
constexpr std::uint32_t kVersion = 1;

/**
 * @brief 最大 Topic 数量
 *
 * 防止文件过大，1024 是合理上限。
 */
constexpr std::uint32_t kMaxTopics = 1024;

/**
 * @brief 写入 16 位整数（大端序）
 */
void Put16(std::ostream& out, std::uint16_t value) {
  out.put(static_cast<char>(value >> 8));
  out.put(static_cast<char>(value));
}

/**
 * @brief 写入 32 位整数（大端序）
 */
void Put32(std::ostream& out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out.put(static_cast<char>(value >> shift));
  }
}

/**
 * @brief 读取 16 位整数（大端序）
 *
 * @param in 输入流
 * @param value 输出参数
 * @return true 读取成功；false 数据不足
 */
bool Get16(std::istream& in, std::uint16_t* value) {
  const int first = in.get();
  const int second = in.get();
  if (first < 0 || second < 0) return false;
  *value = static_cast<std::uint16_t>((first << 8) | second);
  return true;
}

/**
 * @brief 读取 32 位整数（大端序）
 *
 * @param in 输入流
 * @param value 输出参数
 * @return true 读取成功；false 数据不足
 */
bool Get32(std::istream& in, std::uint32_t* value) {
  *value = 0;
  for (int index = 0; index < 4; ++index) {
    const int byte = in.get();
    if (byte < 0) return false;
    *value = (*value << 8) | static_cast<std::uint32_t>(byte);
  }
  return true;
}

/**
 * @brief 设置错误信息
 *
 * @param error 错误信息输出参数
 * @param message 错误消息
 */
void SetError(std::string* error, std::string message) {
  if (error != nullptr) *error = std::move(message);
}

}  // namespace

/**
 * @brief 保存所有 Topic 元数据
 *
 * 保存流程：
 * 1. 参数校验（数量限制、Topic 名称合法性）
 * 2. 创建父目录
 * 3. 创建临时文件
 * 4. 写入文件头（魔数 + 版本 + 数量）
 * 5. 逐条写入 Topic 元数据
 * 6. 原子重命名临时文件为目标文件
 *
 * 文件格式：
 * ```
 * [Magic(4)][Version(4)][Count(4)]
 * [NameLen(2)][Name(N)][PartitionCount(4)]
 * ...
 * ```
 *
 * 原子性保证：
 * - 先写入临时文件
 * - 写入成功后原子重命名
 * - 重命名在 Windows 上使用 MoveFileExW 的 MOVEFILE_REPLACE_EXISTING
 *
 * @param topics 要保存的 Topic 元数据列表
 * @param error 可选的错误信息输出参数
 * @return true 保存成功；false 失败
 */
bool TopicMetadataStore::Save(const std::vector<TopicMetadata>& topics, std::string* error) const {
  // 限制最大 Topic 数量
  if (topics.size() > kMaxTopics) {
    SetError(error, "too many topics in metadata");
    return false;
  }

  // 校验每个 Topic 的合法性
  for (const auto& topic : topics) {
    if (!IsValidTopicName(topic.name) || topic.partition_count == 0 ||
        topic.name.size() > (std::numeric_limits<std::uint16_t>::max)()) {
      SetError(error, "invalid topic metadata");
      return false;
    }
  }

  // 创建父目录
  std::error_code filesystem_error;
  std::filesystem::create_directories(path_.parent_path(), filesystem_error);
  if (filesystem_error) {
    SetError(error, filesystem_error.message());
    return false;
  }

  // 创建临时文件
  auto temporary = path_;
  temporary += ".tmp";
  std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
  if (!out) {
    SetError(error, "cannot open topic metadata temporary file");
    return false;
  }

  // 写入文件头：魔数 "MQTM" + 版本号 + 记录数
  out.write("MQTM", 4);
  Put32(out, kVersion);
  Put32(out, static_cast<std::uint32_t>(topics.size()));

  // 逐条写入 Topic 元数据
  for (const auto& topic : topics) {
    Put16(out, static_cast<std::uint16_t>(topic.name.size()));
    out.write(topic.name.data(), static_cast<std::streamsize>(topic.name.size()));
    Put32(out, topic.partition_count);
  }

  // 刷新并关闭文件
  out.flush();
  out.close();

  // 检查写入是否成功
  if (!out) {
    std::filesystem::remove(temporary, filesystem_error);
    SetError(error, "cannot write topic metadata");
    return false;
  }

  // 原子重命名临时文件为目标文件
#ifdef _WIN32
  if (!MoveFileExW(temporary.c_str(), path_.c_str(),
                   MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    SetError(error, "cannot replace topic metadata");
    return false;
  }
#else
  std::filesystem::rename(temporary, path_, filesystem_error);
  if (filesystem_error) {
    SetError(error, filesystem_error.message());
    return false;
  }
#endif

  return true;
}

/**
 * @brief 加载所有 Topic 元数据
 *
 * 加载流程：
 * 1. 检查文件是否存在
 * 2. 读取并校验文件头（魔数 + 版本 + 数量）
 * 3. 逐条读取 Topic 元数据
 * 4. 校验每个 Topic 的合法性
 *
 * @param topics 输出参数，加载到的 Topic 元数据列表
 * @param error 可选的错误信息输出参数
 * @return true 加载成功（可能返回空列表）；false 文件损坏或 IO 错误
 */
bool TopicMetadataStore::Load(std::vector<TopicMetadata>* topics, std::string* error) const {
  // 空指针检查
  if (topics == nullptr) {
    SetError(error, "topic output is null");
    return false;
  }

  topics->clear();

  // 检查文件是否存在
  std::error_code filesystem_error;
  if (!std::filesystem::exists(path_, filesystem_error)) {
    if (filesystem_error) SetError(error, filesystem_error.message());
    return !filesystem_error;
  }

  // 打开文件
  std::ifstream in(path_, std::ios::binary);

  // 读取并校验文件头
  char magic[4];
  std::uint32_t version = 0;
  std::uint32_t count = 0;
  if (!in.read(magic, 4) || std::string(magic, 4) != "MQTM" || !Get32(in, &version) ||
      version != kVersion || !Get32(in, &count) || count > kMaxTopics) {
    SetError(error, "invalid topic metadata");
    return false;
  }

  // 逐条读取 Topic 元数据
  topics->reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    std::uint16_t name_length = 0;
    std::uint32_t partition_count = 0;

    // 读取名称长度
    if (!Get16(in, &name_length)) {
      SetError(error, "truncated topic metadata");
      return false;
    }

    // 读取名称和分区数
    TopicMetadata topic;
    topic.name.resize(name_length);
    if (!in.read(topic.name.data(), name_length) || !Get32(in, &partition_count)) {
      SetError(error, "truncated topic metadata");
      return false;
    }

    topic.partition_count = partition_count;

    // 校验 Topic 元数据
    if (!IsValidTopicName(topic.name) || topic.partition_count == 0) {
      SetError(error, "invalid topic metadata");
      return false;
    }

    topics->push_back(std::move(topic));
  }

  return true;
}

}  // namespace mq::core