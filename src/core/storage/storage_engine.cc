#include "mq/core/storage_engine.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include "mq/core/topic.h"

namespace mq::core {
namespace {

/**
 * @brief WAL 记录头部大小（字节）
 *
 * WAL 记录格式：
 * [CRC32(4)][BodyLen(4)][Body(N)]
 *
 * 头部包含 CRC32 校验值和 Body 长度，共 8 字节。
 * CRC32 用于崩溃恢复时识别未写完整的损坏尾部。
 */
constexpr std::size_t kRecordHeaderSize = 8;

/**
 * @brief 消息 Body 固定部分大小（字节）
 *
 * Body 格式：
 * [Offset(8)][Timestamp(8)][KeyLen(2)][Key(N)][ValueLen(4)][Value(M)]
 *
 * 固定部分 = 8 + 8 + 2 + 4 = 22 字节
 */
constexpr std::size_t kBodyFixedSize = 22;

/**
 * @brief 单条消息最大字节数（1MB）
 *
 * 防止超大消息导致内存耗尽或性能下降。
 */
constexpr std::uint32_t kMaxMessageBytes = 1024 * 1024;

/**
 * @brief CRC32 校验算法
 *
 * 用于 WAL 记录的完整性校验。
 * 崩溃恢复时，如果 CRC 校验失败，说明该记录未写完整，需要截断。
 *
 * 算法：标准 CRC32（多项式 0xEDB88320）
 *
 * @param data 待校验的数据
 * @return CRC32 校验值
 */
std::uint32_t Crc32(const std::string& data) {
  // CRC 放在记录头部，用于启动恢复时识别未写完整的崩溃尾部。
  std::uint32_t crc = 0xFFFFFFFFu;
  for (unsigned char byte : data) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1));
  }
  return ~crc;
}

/**
 * @brief 写入 16 位整数（大端序）
 *
 * @param out 输出字符串
 * @param value 要写入的值
 */
void Put16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}

/**
 * @brief 写入 32 位整数（大端序）
 *
 * @param out 输出字符串
 * @param value 要写入的值
 */
void Put32(std::string* out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}

/**
 * @brief 写入 64 位整数（大端序）
 *
 * @param out 输出字符串
 * @param value 要写入的值
 */
void Put64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}

/**
 * @brief 读取 16 位整数（大端序）
 *
 * @param data 数据源
 * @param pos 起始位置
 * @return 读取到的值
 */
std::uint16_t Get16(const std::string& data, std::size_t pos) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(data[pos])) << 8) |
         static_cast<unsigned char>(data[pos + 1]);
}

/**
 * @brief 读取 32 位整数（大端序）
 *
 * @param data 数据源
 * @param pos 起始位置
 * @return 读取到的值
 */
std::uint32_t Get32(const std::string& data, std::size_t pos) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}

/**
 * @brief 读取 64 位整数（大端序）
 *
 * @param data 数据源
 * @param pos 起始位置
 * @return 读取到的值
 */
std::uint64_t Get64(const std::string& data, std::size_t pos) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}

/**
 * @brief Topic 名称编码为十六进制
 *
 * 用于文件系统路径，避免特殊字符导致的路径问题。
 * 例如："my-topic" -> "6d792d746f706963"
 *
 * @param topic Topic 名称
 * @return 十六进制编码字符串
 */
std::string TopicCode(const std::string& topic) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(topic.size() * 2);
  for (const unsigned char byte : topic) {
    encoded.push_back(kHex[byte >> 4]);
    encoded.push_back(kHex[byte & 0x0F]);
  }
  return encoded;
}

/**
 * @brief 生成段文件路径
 *
 * 段文件命名格式：00000000000000000001.log
 * 使用 20 位零填充，便于排序。
 *
 * @param directory 分区目录
 * @param number 段编号
 * @return 完整的文件路径
 */
std::filesystem::path SegmentPath(const std::filesystem::path& directory, std::uint64_t number) {
  std::ostringstream name;
  name << std::setw(20) << std::setfill('0') << number << ".log";
  return directory / name.str();
}

/**
 * @brief 刷盘（fsync）指定文件
 *
 * 跨平台实现：Windows 使用 FlushFileBuffers，Linux 使用 fsync。
 * 确保文件数据写入磁盘，而非仅在 OS 缓存中。
 *
 * @param path 文件路径
 * @return true 刷盘成功；false IO 错误
 */
bool SyncPath(const std::filesystem::path& path) {
#ifdef _WIN32
  const auto handle =
      CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                  nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) return false;
  const bool result = FlushFileBuffers(handle) != 0;
  CloseHandle(handle);
  return result;
#else
  const int fd = open(path.c_str(), O_RDWR);
  if (fd < 0) return false;
  const int result = fsync(fd);
  close(fd);
#endif
  return result == 0;
}
}  // namespace

/**
 * @brief 分区内部结构
 *
 * 每个 Topic-Partition 组合对应一个 Partition 实例，
 * 管理该分区的所有段文件、索引和元数据。
 */
struct StorageEngine::Partition {
  /**
   * @brief 段文件结构
   *
   * 每个段文件对应一个 .log 文件和一个 .index 文件。
   * .log 文件存储实际的消息数据，.index 文件存储 offset 到文件位置的映射。
   */
  struct Segment {
    std::uint64_t number = 0;              ///< 段编号
    std::filesystem::path path;            ///< .log 文件路径
    std::filesystem::path index_path;      ///< .index 文件路径
    std::fstream file;                     ///< 文件流
    std::vector<Message> messages;         ///< 内存中的消息缓存
    std::uint64_t size = 0;                ///< 段文件大小（字节）
    std::int64_t last_timestamp_ms = 0;    ///< 最后一条消息的时间戳
  };

  std::string topic;                       ///< Topic 名称
  std::uint32_t number = 0;                ///< 分区号
  std::filesystem::path directory;         ///< 分区目录
  std::vector<std::unique_ptr<Segment>> segments;  ///< 段文件列表
  std::uint64_t next_offset = 0;           ///< 下一个可写 offset
  std::chrono::steady_clock::time_point last_sync = std::chrono::steady_clock::now();  ///< 上次刷盘时间
};

StorageEngine::StorageEngine(std::filesystem::path data_dir)
    : StorageEngine(std::move(data_dir), StorageConfig{}) {}

StorageEngine::StorageEngine(std::filesystem::path data_dir, StorageConfig config)
    : data_dir_(std::move(data_dir)), config_(config) {
  // 校验配置参数，使用默认值填充无效值
  if (config_.segment_size_bytes == 0) config_.segment_size_bytes = 64ULL * 1024 * 1024;
  if (config_.index_interval == 0) config_.index_interval = 1000;
  if (config_.cleaner_interval_ms == 0) config_.cleaner_interval_ms = 1000;
}

StorageEngine::~StorageEngine() {
  // 停止清理线程
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_cleaner_ = true;
  }
  cleaner_cv_.notify_one();
  if (cleaner_thread_.joinable()) cleaner_thread_.join();

  // 刷盘所有未持久化的数据
  Flush(nullptr);
}

bool StorageEngine::Open(std::string* error) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (opened_) return true;

  // 创建队列数据目录
  std::error_code ec;
  std::filesystem::create_directories(data_dir_ / "queues", ec);
  if (ec) {
    if (error) *error = ec.message();
    return false;
  }

  opened_ = true;

  // 启动后台清理线程
  cleaner_thread_ = std::thread(&StorageEngine::CleanerLoop, this);
  return true;
}

/**
 * @brief 获取或创建分区
 *
 * 如果分区不存在，会自动创建目录并执行恢复。
 *
 * @param topic Topic 名称
 * @param partition 分区号
 * @param error 可选的错误信息输出参数
 * @return Partition 指针，失败返回 nullptr
 */
StorageEngine::Partition* StorageEngine::GetPartition(const std::string& topic,
                                                      std::uint32_t partition,
                                                      std::string* error) const {
  // 先在内存中查找
  for (const auto& item : partitions_)
    if (item->topic == topic && item->number == partition) return item.get();

  // 未找到，创建新分区
  auto item = std::make_unique<Partition>();
  item->topic = topic;
  item->number = partition;
  // 目录结构：queues/<topic_hex>/<partition>/
  item->directory = data_dir_ / "queues" / TopicCode(topic) / std::to_string(partition);

  // 创建分区目录
  std::error_code ec;
  std::filesystem::create_directories(item->directory, ec);
  if (ec) {
    if (error) *error = ec.message();
    return nullptr;
  }

  auto* result = item.get();
  partitions_.push_back(std::move(item));

  // 执行恢复：回放 WAL，重建内存状态
  if (!Recover(result, error)) {
    partitions_.pop_back();
    return nullptr;
  }

  return result;
}

/**
 * @brief 恢复分区数据
 *
 * 恢复流程：
 * 1. 扫描分区目录下所有 .log 文件
 * 2. 按段编号排序，从第一个段开始回放
 * 3. 逐条读取 WAL 记录，校验 CRC
 * 4. 验证 offset 连续性
 * 5. 遇到第一条损坏记录时截断其后的尾部
 * 6. 重建内存中的消息缓存和索引
 *
 * @param partition 要恢复的分区
 * @param error 可选的错误信息输出参数
 * @return true 恢复成功；false 恢复失败
 */
bool StorageEngine::Recover(Partition* partition, std::string* error) const {
  // 恢复按段和 offset 顺序扫描；遇到第一条损坏记录时截断其后的尾部。
  std::vector<std::filesystem::path> paths;
  std::error_code ec;

  // 扫描所有 .log 文件
  for (const auto& entry : std::filesystem::directory_iterator(partition->directory, ec))
    if (!ec && entry.path().extension() == ".log") paths.push_back(entry.path());

  // 按文件名排序（即按段编号排序）
  std::sort(paths.begin(), paths.end());

  // 如果没有任何段文件，创建第一个段
  if (paths.empty()) paths.push_back(SegmentPath(partition->directory, 0));

  // 逐个段恢复
  for (const auto& path : paths) {
    auto segment = std::make_unique<Partition::Segment>();
    segment->path = path;

    // 解析段编号
    try {
      segment->number = std::stoull(path.stem().string());
    } catch (...) {
      continue;
    }

    // 设置索引文件路径
    segment->index_path = path;
    segment->index_path.replace_extension(".index");

    // 打开段文件（追加模式）
    segment->file.open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!segment->file) {
      if (error) *error = "cannot open WAL segment";
      return false;
    }

    // 读取文件内容进行恢复
    std::ifstream input(path, std::ios::binary);
    // 重建索引文件
    std::ofstream index(segment->index_path, std::ios::binary | std::ios::trunc);

    std::uint64_t valid_bytes = 0;

    // 逐条读取 WAL 记录
    while (input) {
      // 读取记录头部（8 字节）
      char header[kRecordHeaderSize];
      input.read(header, sizeof(header));
      if (input.gcount() == 0 || input.gcount() != sizeof(header)) break;

      const std::string h(header, sizeof(header));
      const auto crc = Get32(h, 0);      // CRC32 校验值
      const auto len = Get32(h, 4);      // Body 长度

      // 校验 Body 长度合法性
      if (len < kBodyFixedSize || len > kBodyFixedSize + 65535 + kMaxMessageBytes) break;

      // 读取 Body
      std::string body(len, '\0');
      input.read(body.data(), static_cast<std::streamsize>(len));
      if (input.gcount() != static_cast<std::streamsize>(len) || Crc32(body) != crc) break;

      // 解析 Body 字段
      const auto key_len = Get16(body, 16);
      if (18ULL + key_len + 4 > len) break;
      const auto value_len = Get32(body, 18 + key_len);
      if (22ULL + key_len + value_len != len) break;

      // 构造消息对象
      Message msg;
      msg.offset = Get64(body, 0);
      msg.timestamp_ms = static_cast<std::int64_t>(Get64(body, 8));
      msg.key.assign(body, 18, key_len);
      msg.value.assign(body, 22 + key_len, value_len);

      // 校验 offset 连续性，不连续说明有数据损坏
      if (msg.offset != partition->next_offset) break;

      // 定期写入索引条目
      if (msg.offset % config_.index_interval == 0) {
        std::string item;
        Put64(&item, msg.offset);
        Put64(&item, valid_bytes);
        index.write(item.data(), 16);
      }

      // 添加到内存缓存
      segment->messages.push_back(std::move(msg));
      ++partition->next_offset;
      segment->last_timestamp_ms = segment->messages.back().timestamp_ms;
      valid_bytes += kRecordHeaderSize + len;
    }

    // 截断损坏的尾部（如果有的话）
    std::filesystem::resize_file(path, valid_bytes, ec);
    if (ec) {
      if (error) *error = ec.message();
      return false;
    }

    segment->size = valid_bytes;
    partition->segments.push_back(std::move(segment));
  }
  return true;
}

/**
 * @brief 追加消息到指定分区
 *
 * 写入流程：
 * 1. 参数校验
 * 2. 获取或创建分区
 * 3. 构造 WAL 计划（CRC + Body）
 * 4. 检查段文件是否需要滚动
 * 5. 写入 WAL 文件
 * 6. 更新索引（定期）
 * 7. 更新内存缓存
 * 8. 根据 fsync 策略决定是否刷盘
 *
 * WAL 记录格式：
 * [CRC32(4)][BodyLen(4)][Body(N)]
 *
 * Body 格式：
 * [Offset(8)][Timestamp(8)][KeyLen(2)][Key(N)][ValueLen(4)][Value(M)]
 *
 * @param topic Topic 名称
 * @param partition 分区号
 * @param key 消息键
 * @param value 消息值
 * @param message 输出参数，成功时写入完整的消息信息
 * @param error 可选的错误信息输出参数
 * @return true 写入成功；false 失败
 */
bool StorageEngine::Append(const std::string& topic, std::uint32_t partition, std::string key,
                           std::string value, Message* message, std::string* error) {
  // 参数校验
  if (!IsValidTopicName(topic) || message == nullptr || key.size() > 65535 || value.empty() ||
      value.size() > kMaxMessageBytes) {
    if (error) *error = "invalid message";
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // 延迟初始化：如果还没打开，先创建目录
  if (!opened_) {
    std::error_code ec;
    std::filesystem::create_directories(data_dir_ / "queues", ec);
    if (ec) {
      if (error) *error = ec.message();
      return false;
    }
    opened_ = true;
  }

  // 获取或创建分区
  auto* target = GetPartition(topic, partition, error);
  if (!target) return false;

  auto& active = target->segments.back();

  // 只滚动已写入数据的段，避免空段反复创建；每个段都保留独立索引便于恢复。

  // 构造消息对象
  Message next;
  next.offset = target->next_offset;
  next.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  next.key = std::move(key);
  next.value = std::move(value);

  // 序列化消息 Body
  std::string body;
  Put64(&body, next.offset);
  Put64(&body, static_cast<std::uint64_t>(next.timestamp_ms));
  Put16(&body, static_cast<std::uint16_t>(next.key.size()));
  body.append(next.key);
  Put32(&body, static_cast<std::uint32_t>(next.value.size()));
  body.append(next.value);

  // 构造完整记录：[CRC32(4)][BodyLen(4)][Body(N)]
  std::string record;
  Put32(&record, Crc32(body));
  Put32(&record, static_cast<std::uint32_t>(body.size()));
  record.append(body);

  // 检查是否需要滚动段文件
  if (active->size != 0 && active->size + record.size() > config_.segment_size_bytes) {
    auto segment = std::make_unique<Partition::Segment>();
    segment->number = active->number + 1;
    segment->path = SegmentPath(target->directory, segment->number);
    segment->index_path = segment->path;
    segment->index_path.replace_extension(".index");
    segment->file.open(segment->path,
                       std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!segment->file) {
      if (error) *error = "cannot create WAL segment";
      return false;
    }
    target->segments.push_back(std::move(segment));
  }

  auto& current = target->segments.back();
  const auto position = current->size;

  // 写入 WAL 文件
  current->file.clear();
  current->file.seekp(0, std::ios::end);
  current->file.write(record.data(), static_cast<std::streamsize>(record.size()));
  if (!current->file) {
    if (error) *error = "WAL write failed";
    return false;
  }

  // 定期写入索引
  if (next.offset % config_.index_interval == 0) {
    std::ofstream index(current->index_path, std::ios::binary | std::ios::app);
    std::string item;
    Put64(&item, next.offset);
    Put64(&item, position);
    index.write(item.data(), 16);
  }

  // 更新内存状态
  current->size += record.size();
  current->last_timestamp_ms = next.timestamp_ms;
  current->messages.push_back(next);
  ++target->next_offset;

  // 刷新文件缓冲区
  current->file.flush();

  // 根据 fsync 策略决定是否刷盘
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - target->last_sync)
                           .count();
  const bool sync =
      config_.fsync_policy == FsyncPolicy::kPerMessage ||
      config_.fsync_policy == FsyncPolicy::kPerBatch ||
      (config_.fsync_policy == FsyncPolicy::kInterval && elapsed >= config_.fsync_interval_ms);

  const bool index_exists = std::filesystem::exists(current->index_path);
  if (sync && (!SyncPath(current->path) || (index_exists && !SyncPath(current->index_path)))) {
    if (error) *error = "WAL fsync failed";
    return false;
  }
  if (sync) target->last_sync = std::chrono::steady_clock::now();

  *message = next;
  return true;
}

/**
 * @brief 副本追加，要求 offset 连续
 *
 * 与 Append 类似，但额外要求 offset 必须连续。
 * 用于主从复制场景，Follower 节点接收 Leader 的日志。
 *
 * offset 不连续说明有数据丢失或复制缺口，此时拒绝写入并返回错误。
 *
 * @param topic Topic 名称
 * @param partition 分区号
 * @param message 要追加的消息（必须携带正确的 offset）
 * @param error 可选的错误信息输出参数
 * @return true 追加成功；false offset 不连续或存储错误
 */
bool StorageEngine::AppendReplica(const std::string& topic, std::uint32_t partition,
                                  const Message& message, std::string* error) {
  // 参数校验
  if (!IsValidTopicName(topic) || message.key.size() > 65535 || message.value.empty() ||
      message.value.size() > kMaxMessageBytes) {
    if (error) *error = "invalid message";
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // 延迟初始化
  if (!opened_) {
    std::error_code ec;
    std::filesystem::create_directories(data_dir_ / "queues", ec);
    if (ec) {
      if (error) *error = ec.message();
      return false;
    }
    opened_ = true;
  }

  // 获取分区
  auto* target = GetPartition(topic, partition, error);
  if (target == nullptr) return false;

  // 校验 offset 连续性：副本追加必须严格连续
  if (message.offset != target->next_offset) {
    if (error) *error = "replica offset gap";
    return false;
  }

  // 序列化消息 Body
  std::string body;
  Put64(&body, message.offset);
  Put64(&body, static_cast<std::uint64_t>(message.timestamp_ms));
  Put16(&body, static_cast<std::uint16_t>(message.key.size()));
  body.append(message.key);
  Put32(&body, static_cast<std::uint32_t>(message.value.size()));
  body.append(message.value);

  // 构造完整记录
  std::string record;
  Put32(&record, Crc32(body));
  Put32(&record, static_cast<std::uint32_t>(body.size()));
  record.append(body);

  auto& active = target->segments.back();

  // 检查段文件滚动
  if (active->size != 0 && active->size + record.size() > config_.segment_size_bytes) {
    auto segment = std::make_unique<Partition::Segment>();
    segment->number = active->number + 1;
    segment->path = SegmentPath(target->directory, segment->number);
    segment->index_path = segment->path;
    segment->index_path.replace_extension(".index");
    segment->file.open(segment->path,
                       std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!segment->file) {
      if (error) *error = "cannot create WAL segment";
      return false;
    }
    target->segments.push_back(std::move(segment));
  }

  auto& current = target->segments.back();
  const auto position = current->size;

  // 写入 WAL 文件
  current->file.clear();
  current->file.seekp(0, std::ios::end);
  current->file.write(record.data(), static_cast<std::streamsize>(record.size()));
  if (!current->file) {
    if (error) *error = "WAL write failed";
    return false;
  }

  // 更新索引
  if (message.offset % config_.index_interval == 0) {
    std::ofstream index(current->index_path, std::ios::binary | std::ios::app);
    std::string item;
    Put64(&item, message.offset);
    Put64(&item, position);
    index.write(item.data(), 16);
  }

  // 更新内存状态
  current->size += record.size();
  current->last_timestamp_ms = message.timestamp_ms;
  current->messages.push_back(message);
  ++target->next_offset;

  current->file.flush();

  // 根据 fsync 策略刷盘
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - target->last_sync)
                           .count();
  const bool sync =
      config_.fsync_policy == FsyncPolicy::kPerMessage ||
      config_.fsync_policy == FsyncPolicy::kPerBatch ||
      (config_.fsync_policy == FsyncPolicy::kInterval && elapsed >= config_.fsync_interval_ms);

  const bool index_exists = std::filesystem::exists(current->index_path);
  if (sync && (!SyncPath(current->path) || (index_exists && !SyncPath(current->index_path)))) {
    if (error) *error = "WAL fsync failed";
    return false;
  }
  if (sync) target->last_sync = std::chrono::steady_clock::now();

  return true;
}

/**
 * @brief 删除 Topic 的所有分区数据
 *
 * 删除流程：
 * 1. 从内存中移除所有相关分区
 * 2. 删除文件系统中的整个 Topic 目录
 *
 * @param topic Topic 名称
 * @param error 可选的错误信息输出参数
 * @return true 删除成功；false 失败
 */
bool StorageEngine::DeleteTopic(const std::string& topic, std::string* error) {
  if (!IsValidTopicName(topic)) {
    if (error) *error = "invalid topic";
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // 从内存中移除该 Topic 的所有分区
  for (auto it = partitions_.begin(); it != partitions_.end();) {
    if ((*it)->topic == topic)
      it = partitions_.erase(it);
    else
      ++it;
  }

  // 删除文件系统中的目录
  std::error_code ec;
  std::filesystem::remove_all(data_dir_ / "queues" / TopicCode(topic), ec);
  if (ec) {
    if (error) *error = ec.message();
    return false;
  }

  return true;
}

/**
 * @brief 从指定分区读取消息
 *
 * 读取流程：
 * 1. 获取分区
 * 2. 遍历所有段文件的消息缓存
 * 3. 跳过 offset < start_offset 的消息
 * 4. 累计字节数，超过 max_bytes 时停止
 *
 * 注意：消息从内存缓存中读取，不进行磁盘 IO。
 *
 * @param topic Topic 名称
 * @param partition 分区号
 * @param start_offset 起始 offset（inclusive）
 * @param max_bytes 单次返回的最大字节数
 * @param messages 输出参数，读取到的消息列表
 * @param error 可选的错误信息输出参数
 * @return true 读取成功；false 参数无效
 */
bool StorageEngine::Read(const std::string& topic, std::uint32_t partition,
                         std::uint64_t start_offset, std::uint32_t max_bytes,
                         std::vector<Message>* messages, std::string* error) const {
  if (!IsValidTopicName(topic) || messages == nullptr || max_bytes == 0) return false;

  std::lock_guard<std::mutex> lock(mutex_);

  auto* target = GetPartition(topic, partition, error);
  if (!target) return false;

  // 校验 offset 有效性
  if (start_offset > target->next_offset) {
    if (error) *error = "invalid offset";
    return false;
  }

  messages->clear();
  std::uint64_t bytes = 0;

  // 遍历所有段文件的消息
  for (const auto& segment : target->segments)
    for (const auto& item : segment->messages) {
      // 跳过已消费的消息
      if (item.offset < start_offset) continue;

      const auto size = item.key.size() + item.value.size();

      // 检查是否超过字节限制
      if (!messages->empty() && bytes + size > max_bytes) return true;
      if (size > max_bytes) return true;

      messages->push_back(item);
      bytes += size;
    }

  return true;
}

/**
 * @brief 获取指定分区的下一个可写 offset
 *
 * @param topic Topic 名称
 * @param partition 分区号
 * @param offset 输出参数，下一个可写 offset
 * @param error 可选的错误信息输出参数
 * @return true 查询成功；false 参数无效
 */
bool StorageEngine::NextOffset(const std::string& topic, std::uint32_t partition,
                               std::uint64_t* offset, std::string* error) const {
  if (!IsValidTopicName(topic) || offset == nullptr) return false;

  std::lock_guard<std::mutex> lock(mutex_);

  auto* target = GetPartition(topic, partition, error);
  if (target == nullptr) return false;

  *offset = target->next_offset;
  return true;
}

/**
 * @brief 强制刷盘，将所有未持久化的数据写入磁盘
 *
 * 遍历所有分区和段文件，执行 fs


    

2 of  3 
233  9


.:.制 System.md全域
/**
->同步  bSegments(part >. fs// stdstd::lock_guard<std::mutex> lock(mutex_);

  for (const auto& partition : partitions_)
    for (const auto& segment : partition->segments) {
           segment->file.flush();
           if (!segment->file || !SyncPath(segment->path) ||
                   (std::filesystem::exists(segment->index_path) && !SyncPath(segment->index_path))) {
               if (error) *error = "WAL flush failed";
        return false;
      }
    }

   return true;
}

/**
 * @brief 手动触发过期段文件清理
 *
 * 清理策略：
 * 1. 时间过期：消息时间戳超过 retention_ms
 * 2. 大小超限：分区总大小超过 retention_bytes
 *
 * 注意：至少保留一个段文件，即使它已过期。
 *
 * @param error 可选的错误信息输出参数
 * @return true 清理成功；false 删除文件失败
 */
bool StorageEngine::CleanupExpiredSegments(std::string* error) {
  std::lock_guard<std::mutex> lock(mutex_);

  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

  for (const auto& partition : partitions_) {
    // 计算分区总大小
    std::uint64_t total = 0;
    for (const auto& segment : partition->segments) total += segment->size;

    // 从最旧的段开始清理
    while (partition->segments.size() > 1) {
      const auto& oldest = partition->segments.front();

      // 检查时间过期
      const bool expired =
          config_.retention_ms != 0 && oldest->last_timestamp_ms != 0 &&
          now - oldest->last_timestamp_ms >= static_cast<std::int64_t>(config_.retention_ms);

      // 检查大小超限
      const bool oversized = config_.retention_bytes != 0 && total > config_.retention_bytes;

      if (!expired && !oversized) break;

      // 删除段文件
      total -= oldest->size;
      oldest->file.close();
      std::error_code ec;
      std::filesystem::remove(oldest->path, ec);
      if (!ec) std::filesystem::remove(oldest->index_path, ec);
      if (ec) {
        if (error) *error = ec.message();
        return false;
      }

      partition->segments.erase(partition->segments.begin());
    }
  }

  return true;
}

/**
 * @brief 后台清理线程主函数
 *
 * 周期性扫描所有分区，删除满足过期条件的段文件。
 * 使用条件变量等待，支持优雅退出。
 *
 * 异常处理：捕获所有异常，确保清理线程不会导致 Broker 崩溃。
 */
void StorageEngine::CleanerLoop() {
  try {
    while (true) {
      std::unique_lock<std::mutex> lock(mutex_);
      // 等待超时或收到停止信号
      if (cleaner_cv_.wait_for(lock, std::chrono::milliseconds(config_.cleaner_interval_ms),
                               [this] { return stop_cleaner_; }))
        return;
      lock.unlock();

      // 执行清理（不持有锁，避免阻塞写操作）
      CleanupExpiredSegments(nullptr);
    }
  } catch (...) {
    // Retention cleanup is best effort and must not terminate the Broker.
  }
}
}  // namespace mq::core