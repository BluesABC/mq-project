#include "mq/core/consumer_offset_store.h"

#include <fstream>
#include <limits>
#ifdef _WIN32
#include <windows.h>
#endif

namespace mq::core {
namespace {

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
  for (int shift = 24; shift >= 0; shift -= 8) out.put(static_cast<char>(value >> shift));
}

/**
 * @brief 写入 64 位整数（大端序）
 */
void Put64(std::ostream& out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out.put(static_cast<char>(value >> shift));
}

/**
 * @brief 读取 16 位整数（大端序）
 *
 * @param in 输入流
 * @param value 输出参数
 * @return true 读取成功；false 数据不足
 */
bool Get16(std::istream& in, std::uint16_t* value) {
  int a = in.get(), b = in.get();
  if (a < 0 || b < 0) return false;
  *value = static_cast<std::uint16_t>((a << 8) | b);
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
  for (int i = 0; i < 4; ++i) {
    int b = in.get();
    if (b < 0) return false;
    *value = (*value << 8) | static_cast<std::uint32_t>(b);
  }
  return true;
}

/**
 * @brief 读取 64 位整数（大端序）
 *
 * @param in 输入流
 * @param value 输出参数
 * @return true 读取成功；false 数据不足
 */
bool Get64(std::istream& in, std::uint64_t* value) {
  *value = 0;
  for (int i = 0; i < 8; ++i) {
    int b = in.get();
    if (b < 0) return false;
    *value = (*value << 8) | static_cast<std::uint64_t>(b);
  }
  return true;
}

/**
 * @brief 设置错误信息
 *
 * @param error 错误信息输出参数
 * @param message 错误消息
 */
void Error(std::string* error, const char* message) {
  if (error) *error = message;
}

}  // namespace

/**
 * @brief 批量保存消费者偏移量
 *
 * 保存流程：
 * 1. 参数校验（数量限制）
 * 2. 创建临时文件
 * 3. 写入文件头（魔数 + 版本 + 数量）
 * 4. 逐条写入偏移量记录
 * 5. 原子重命名临时文件为目标文件
 *
 * 文件格式：
 * ```
 * [Magic(4)][Version(4)][Count(4)]
 * [GroupLen(2)][Group(N)][TopicLen(2)][Topic(N)][Partition(4)][Offset(8)]
 * ...
 * ```
 *
 * 原子性保证：
 * - 先写入临时文件
 * - 写入成功后原子重命名
 * - 重命名在 Windows 上使用 MoveFileExW 的 MOVEFILE_REPLACE_EXISTING
 *
 * @param offsets 要保存的偏移量列表
 * @param error 可选的错误信息输出参数
 * @return true 保存成功；false 失败
 */
bool ConsumerOffsetStore::Save(const std::vector<ConsumerOffset>& offsets,
                               std::string* error) const {
  // 限制最大记录数，防止文件过大
  if (offsets.size() > 100000) {
    Error(error, "too many consumer offsets");
    return false;
  }

  // 创建父目录
  std::error_code ec;
  std::filesystem::create_directories(path_.parent_path(), ec);
  if (ec) {
    if (error) *error = ec.message();
    return false;
  }

  // 创建临时文件
  auto temporary = path_;
  temporary += ".tmp";
  std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
  if (!out) {
    Error(error, "cannot open consumer offset temporary file");
    return false;
  }

  // 写入文件头：魔数 "MQCO" + 版本号 + 记录数
  out.write("MQCO", 4);
  Put32(out, 1);  // 版本号
  Put32(out, static_cast<std::uint32_t>(offsets.size()));

  // 逐条写入偏移量记录
  for (const auto& item : offsets) {
    // 校验字符串长度
    if (item.group.size() > 65535 || item.topic.size() > 65535) {
      Error(error, "consumer offset name too long");
      return false;
    }

    // 写入 group
    Put16(out, static_cast<std::uint16_t>(item.group.size()));
    out.write(item.group.data(), item.group.size());

    // 写入 topic
    Put16(out, static_cast<std::uint16_t>(item.topic.size()));
    out.write(item.topic.data(), item.topic.size());

    // 写入 partition 和 offset
    Put32(out, item.partition);
    Put64(out, item.offset);
  }

  // 刷新并关闭文件
  out.flush();
  out.close();

  // 检查写入是否成功
  if (!out) {
    std::filesystem::remove(temporary, ec);
    Error(error, "cannot write consumer offsets");
    return false;
  }

  // 原子重命名临时文件为目标文件
#ifdef _WIN32
  if (!MoveFileExW(temporary.c_str(), path_.c_str(),
                   MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    Error(error, "cannot replace consumer offsets");
    return false;
  }
#else
  std::filesystem::rename(temporary, path_, ec);
  if (ec) {
    if (error) *error = ec.message();
    return false;
  }
#endif

  return true;
}

/**
 * @brief 加载所有消费者偏移量
 *
 * 加载流程：
 * 1. 检查文件是否存在
 * 2. 读取并校验文件头（魔数 + 版本 + 数量）
 * 3. 逐条读取偏移量记录
 * 4. 校验数据完整性
 *
 * @param offsets 输出参数，加载到的偏移量列表
 * @param error 可选的错误信息输出参数
 * @return true 加载成功（可能返回空列表）；false 文件损坏或 IO 错误
 */
bool ConsumerOffsetStore::Load(std::vector<ConsumerOffset>* offsets, std::string* error) const {
  // 空指针检查
  if (!offsets) {
    Error(error, "consumer offset output is null");
    return false;
  }

  offsets->clear();

  // 检查文件是否存在
  std::error_code ec;
  if (!std::filesystem::exists(path_, ec)) return !ec;

  // 打开文件
  std::ifstream in(path_, std::ios::binary);

  // 读取并校验文件头
  char magic[4];
  std::uint32_t version = 0, count = 0;
  if (!in.read(magic, 4) || std::string(magic, 4) != "MQCO" || !Get32(in, &version) ||
      version != 1 || !Get32(in, &count) || count > 100000) {
    Error(error, "invalid consumer offsets");
    return false;
  }

  // 逐条读取偏移量记录
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint16_t group_size = 0, topic_size = 0;
    ConsumerOffset item;

    // 读取 group
    if (!Get16(in, &group_size) || group_size > 65535) {
      Error(error, "truncated consumer offsets");
      return false;
    }
    item.group.resize(group_size);
    if (!in.read(item.group.data(), group_size) || !Get16(in, &topic_size)) {
      Error(error, "truncated consumer offsets");
      return false;
    }

    // 读取 topic
    item.topic.resize(topic_size);
    if (!in.read(item.topic.data(), topic_size) || !Get32(in, &item.partition) ||
        !Get64(in, &item.offset)) {
      Error(error, "truncated consumer offsets");
      return false;
    }

    offsets->push_back(std::move(item));
  }

  return true;
}

}  // namespace mq::core