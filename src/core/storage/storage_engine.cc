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
constexpr std::size_t kRecordHeaderSize = 8;
constexpr std::size_t kBodyFixedSize = 22;
constexpr std::uint32_t kMaxMessageBytes = 1024 * 1024;

std::uint32_t Crc32(const std::string& data) {
  // CRC 放在记录头部，用于启动恢复时识别未写完整的崩溃尾部。
  std::uint32_t crc = 0xFFFFFFFFu;
  for (unsigned char byte : data) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1));
  }
  return ~crc;
}
void Put16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}
void Put32(std::string* out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}
void Put64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out->push_back(static_cast<char>(value >> shift));
}
std::uint16_t Get16(const std::string& data, std::size_t pos) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(data[pos])) << 8) |
         static_cast<unsigned char>(data[pos + 1]);
}
std::uint32_t Get32(const std::string& data, std::size_t pos) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}
std::uint64_t Get64(const std::string& data, std::size_t pos) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(data[pos + i]);
  return value;
}
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
std::filesystem::path SegmentPath(const std::filesystem::path& directory, std::uint64_t number) {
  std::ostringstream name;
  name << std::setw(20) << std::setfill('0') << number << ".log";
  return directory / name.str();
}
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
  const int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  const int result = fsync(fd);
  close(fd);
#endif
  return result == 0;
}
}  // namespace

struct StorageEngine::Partition {
  struct Segment {
    std::uint64_t number = 0;
    std::filesystem::path path;
    std::filesystem::path index_path;
    std::fstream file;
    std::vector<Message> messages;
    std::uint64_t size = 0;
    std::int64_t last_timestamp_ms = 0;
  };
  std::string topic;
  std::uint32_t number = 0;
  std::filesystem::path directory;
  std::vector<std::unique_ptr<Segment>> segments;
  std::uint64_t next_offset = 0;
  std::chrono::steady_clock::time_point last_sync = std::chrono::steady_clock::now();
};

StorageEngine::StorageEngine(std::filesystem::path data_dir)
    : StorageEngine(std::move(data_dir), StorageConfig{}) {}
StorageEngine::StorageEngine(std::filesystem::path data_dir, StorageConfig config)
    : data_dir_(std::move(data_dir)), config_(config) {
  if (config_.segment_size_bytes == 0) config_.segment_size_bytes = 64ULL * 1024 * 1024;
  if (config_.index_interval == 0) config_.index_interval = 1000;
  if (config_.cleaner_interval_ms == 0) config_.cleaner_interval_ms = 1000;
}
StorageEngine::~StorageEngine() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_cleaner_ = true;
  }
  cleaner_cv_.notify_one();
  if (cleaner_thread_.joinable()) cleaner_thread_.join();
  Flush(nullptr);
}

bool StorageEngine::Open(std::string* error) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (opened_) return true;
  std::error_code ec;
  std::filesystem::create_directories(data_dir_ / "queues", ec);
  if (ec) {
    if (error) *error = ec.message();
    return false;
  }
  opened_ = true;
  cleaner_thread_ = std::thread(&StorageEngine::CleanerLoop, this);
  return true;
}

StorageEngine::Partition* StorageEngine::GetPartition(const std::string& topic,
                                                      std::uint32_t partition,
                                                      std::string* error) const {
  for (const auto& item : partitions_)
    if (item->topic == topic && item->number == partition) return item.get();
  auto item = std::make_unique<Partition>();
  item->topic = topic;
  item->number = partition;
  item->directory = data_dir_ / "queues" / TopicCode(topic) / std::to_string(partition);
  std::error_code ec;
  std::filesystem::create_directories(item->directory, ec);
  if (ec) {
    if (error) *error = ec.message();
    return nullptr;
  }
  auto* result = item.get();
  partitions_.push_back(std::move(item));
  if (!Recover(result, error)) {
    partitions_.pop_back();
    return nullptr;
  }
  return result;
}

bool StorageEngine::Recover(Partition* partition, std::string* error) const {
  // 恢复按段和 offset 顺序扫描；遇到第一条损坏记录时截断其后的尾部。
  std::vector<std::filesystem::path> paths;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(partition->directory, ec))
    if (!ec && entry.path().extension() == ".log") paths.push_back(entry.path());
  std::sort(paths.begin(), paths.end());
  if (paths.empty()) paths.push_back(SegmentPath(partition->directory, 0));
  for (const auto& path : paths) {
    auto segment = std::make_unique<Partition::Segment>();
    segment->path = path;
    try {
      segment->number = std::stoull(path.stem().string());
    } catch (...) {
      continue;
    }
    segment->index_path = path;
    segment->index_path.replace_extension(".index");
    segment->file.open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!segment->file) {
      if (error) *error = "cannot open WAL segment";
      return false;
    }
    std::ifstream input(path, std::ios::binary);
    std::ofstream index(segment->index_path, std::ios::binary | std::ios::trunc);
    std::uint64_t valid_bytes = 0;
    while (input) {
      char header[kRecordHeaderSize];
      input.read(header, sizeof(header));
      if (input.gcount() == 0 || input.gcount() != sizeof(header)) break;
      const std::string h(header, sizeof(header));
      const auto crc = Get32(h, 0);
      const auto len = Get32(h, 4);
      if (len < kBodyFixedSize || len > kBodyFixedSize + 65535 + kMaxMessageBytes) break;
      std::string body(len, '\0');
      input.read(body.data(), static_cast<std::streamsize>(len));
      if (input.gcount() != static_cast<std::streamsize>(len) || Crc32(body) != crc) break;
      const auto key_len = Get16(body, 16);
      if (18ULL + key_len + 4 > len) break;
      const auto value_len = Get32(body, 18 + key_len);
      if (22ULL + key_len + value_len != len) break;
      Message msg;
      msg.offset = Get64(body, 0);
      msg.timestamp_ms = static_cast<std::int64_t>(Get64(body, 8));
      msg.key.assign(body, 18, key_len);
      msg.value.assign(body, 22 + key_len, value_len);
      if (msg.offset != partition->next_offset) break;
      if (msg.offset % config_.index_interval == 0) {
        std::string item;
        Put64(&item, msg.offset);
        Put64(&item, valid_bytes);
        index.write(item.data(), 16);
      }
      segment->messages.push_back(std::move(msg));
      ++partition->next_offset;
      segment->last_timestamp_ms = segment->messages.back().timestamp_ms;
      valid_bytes += kRecordHeaderSize + len;
    }
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

bool StorageEngine::Append(const std::string& topic, std::uint32_t partition, std::string key,
                           std::string value, Message* message, std::string* error) {
  if (!IsValidTopicName(topic) || message == nullptr || key.size() > 65535 || value.empty() ||
      value.size() > kMaxMessageBytes) {
    if (error) *error = "invalid message";
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (!opened_) {
    std::error_code ec;
    std::filesystem::create_directories(data_dir_ / "queues", ec);
    if (ec) {
      if (error) *error = ec.message();
      return false;
    }
    opened_ = true;
  }
  auto* target = GetPartition(topic, partition, error);
  if (!target) return false;
  auto& active = target->segments.back();
  // 只滚动已写入数据的段，避免空段反复创建；每个段都保留独立索引便于恢复。
  Message next;
  next.offset = target->next_offset;
  next.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  next.key = std::move(key);
  next.value = std::move(value);
  std::string body;
  Put64(&body, next.offset);
  Put64(&body, static_cast<std::uint64_t>(next.timestamp_ms));
  Put16(&body, static_cast<std::uint16_t>(next.key.size()));
  body.append(next.key);
  Put32(&body, static_cast<std::uint32_t>(next.value.size()));
  body.append(next.value);
  std::string record;
  Put32(&record, Crc32(body));
  Put32(&record, static_cast<std::uint32_t>(body.size()));
  record.append(body);
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
  current->file.clear();
  current->file.seekp(0, std::ios::end);
  current->file.write(record.data(), static_cast<std::streamsize>(record.size()));
  if (!current->file) {
    if (error) *error = "WAL write failed";
    return false;
  }
  if (next.offset % config_.index_interval == 0) {
    std::ofstream index(current->index_path, std::ios::binary | std::ios::app);
    std::string item;
    Put64(&item, next.offset);
    Put64(&item, position);
    index.write(item.data(), 16);
  }
  current->size += record.size();
  current->last_timestamp_ms = next.timestamp_ms;
  current->messages.push_back(next);
  ++target->next_offset;
  current->file.flush();
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - target->last_sync)
                           .count();
  const bool sync =
      config_.fsync_policy == FsyncPolicy::kPerMessage ||
      (config_.fsync_policy == FsyncPolicy::kInterval && elapsed >= config_.fsync_interval_ms);
  if (sync && (!SyncPath(current->path) || !SyncPath(current->index_path))) {
    if (error) *error = "WAL fsync failed";
    return false;
  }
  if (sync) target->last_sync = std::chrono::steady_clock::now();
  *message = next;
  return true;
}

bool StorageEngine::AppendReplica(const std::string& topic, std::uint32_t partition,
                                  const Message& message, std::string* error) {
  if (!IsValidTopicName(topic) || message.key.size() > 65535 || message.value.empty() ||
      message.value.size() > kMaxMessageBytes) {
    if (error) *error = "invalid message";
    return false;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (!opened_) {
    std::error_code ec;
    std::filesystem::create_directories(data_dir_ / "queues", ec);
    if (ec) {
      if (error) *error = ec.message();
      return false;
    }
    opened_ = true;
  }
  auto* target = GetPartition(topic, partition, error);
  if (target == nullptr) return false;
  if (message.offset != target->next_offset) {
    if (error) *error = "replica offset gap";
    return false;
  }
  std::string body;
  Put64(&body, message.offset);
  Put64(&body, static_cast<std::uint64_t>(message.timestamp_ms));
  Put16(&body, static_cast<std::uint16_t>(message.key.size()));
  body.append(message.key);
  Put32(&body, static_cast<std::uint32_t>(message.value.size()));
  body.append(message.value);
  std::string record;
  Put32(&record, Crc32(body));
  Put32(&record, static_cast<std::uint32_t>(body.size()));
  record.append(body);
  auto& active = target->segments.back();
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
  current->file.clear();
  current->file.seekp(0, std::ios::end);
  current->file.write(record.data(), static_cast<std::streamsize>(record.size()));
  if (!current->file) {
    if (error) *error = "WAL write failed";
    return false;
  }
  if (message.offset % config_.index_interval == 0) {
    std::ofstream index(current->index_path, std::ios::binary | std::ios::app);
    std::string item;
    Put64(&item, message.offset);
    Put64(&item, position);
    index.write(item.data(), 16);
  }
  current->size += record.size();
  current->last_timestamp_ms = message.timestamp_ms;
  current->messages.push_back(message);
  ++target->next_offset;
  current->file.flush();
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - target->last_sync)
                           .count();
  const bool sync =
      config_.fsync_policy == FsyncPolicy::kPerMessage ||
      (config_.fsync_policy == FsyncPolicy::kInterval && elapsed >= config_.fsync_interval_ms);
  if (sync && (!SyncPath(current->path) || !SyncPath(current->index_path))) {
    if (error) *error = "WAL fsync failed";
    return false;
  }
  if (sync) target->last_sync = std::chrono::steady_clock::now();
  return true;
}

bool StorageEngine::Read(const std::string& topic, std::uint32_t partition,
                         std::uint64_t start_offset, std::uint32_t max_bytes,
                         std::vector<Message>* messages, std::string* error) const {
  if (!IsValidTopicName(topic) || messages == nullptr || max_bytes == 0) return false;
  std::lock_guard<std::mutex> lock(mutex_);
  auto* target = GetPartition(topic, partition, error);
  if (!target) return false;
  messages->clear();
  std::uint64_t bytes = 0;
  for (const auto& segment : target->segments)
    for (const auto& item : segment->messages) {
      if (item.offset < start_offset) continue;
      const auto size = item.key.size() + item.value.size();
      if (!messages->empty() && bytes + size > max_bytes) return true;
      if (size > max_bytes) return true;
      messages->push_back(item);
      bytes += size;
    }
  return true;
}

bool StorageEngine::Flush(std::string* error) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& partition : partitions_)
    for (const auto& segment : partition->segments) {
      segment->file.flush();
      if (!segment->file || !SyncPath(segment->path) || !SyncPath(segment->index_path)) {
        if (error) *error = "WAL flush failed";
        return false;
      }
    }
  return true;
}

bool StorageEngine::CleanupExpiredSegments(std::string* error) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  for (const auto& partition : partitions_) {
    std::uint64_t total = 0;
    for (const auto& segment : partition->segments) total += segment->size;
    while (partition->segments.size() > 1) {
      const auto& oldest = partition->segments.front();
      const bool expired =
          config_.retention_ms != 0 && oldest->last_timestamp_ms != 0 &&
          now - oldest->last_timestamp_ms >= static_cast<std::int64_t>(config_.retention_ms);
      const bool oversized = config_.retention_bytes != 0 && total > config_.retention_bytes;
      if (!expired && !oversized) break;
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

void StorageEngine::CleanerLoop() {
  try {
    while (true) {
      std::unique_lock<std::mutex> lock(mutex_);
      if (cleaner_cv_.wait_for(lock, std::chrono::milliseconds(config_.cleaner_interval_ms),
                               [this] { return stop_cleaner_; }))
        return;
      lock.unlock();
      CleanupExpiredSegments(nullptr);
    }
  } catch (...) {
    // Retention cleanup is best effort and must not terminate the Broker.
  }
}
}  // namespace mq::core
