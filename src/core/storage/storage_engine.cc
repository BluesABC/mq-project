#include "mq/core/storage_engine.h"

#include <chrono>
#include <fstream>
#include <utility>

namespace mq::core {
namespace {

constexpr std::size_t kRecordHeaderSize = 8;
constexpr std::size_t kBodyFixedSize = 8 + 8 + 2 + 4;
constexpr std::uint32_t kMaxMessageBytes = 1024 * 1024;

std::uint32_t Crc32(const std::string& data) {
  std::uint32_t crc = 0xFFFFFFFFu;
  for (unsigned char byte : data) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xEDB88320u & -(crc & 1));
  }
  return ~crc;
}
void Put16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value >> 8)); out->push_back(static_cast<char>(value));
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
std::string PathFor(const std::filesystem::path& root, const std::string& topic,
                   std::uint32_t partition) {
  return (root / "queues" / topic / (std::to_string(partition) + ".log")).string();
}

}  // namespace

struct StorageEngine::Partition {
  std::string topic;
  std::uint32_t number = 0;
  std::filesystem::path path;
  std::fstream file;
  std::vector<Message> messages;
};

StorageEngine::StorageEngine(std::filesystem::path data_dir) : data_dir_(std::move(data_dir)) {}
StorageEngine::~StorageEngine() { Flush(nullptr); }

bool StorageEngine::Open(std::string* error) {
  std::lock_guard lock(mutex_);
  if (opened_) return true;
  std::error_code ec;
  std::filesystem::create_directories(data_dir_ / "queues", ec);
  if (ec) { if (error != nullptr) *error = ec.message(); return false; }
  opened_ = true;
  return true;
}

StorageEngine::Partition* StorageEngine::GetPartition(const std::string& topic,
                                                       std::uint32_t partition,
                                                       std::string* error) const {
  for (const auto& item : partitions_) if (item->topic == topic && item->number == partition) return item.get();
  auto item = std::make_unique<Partition>(); item->topic = topic; item->number = partition;
  item->path = PathFor(data_dir_, topic, partition);
  std::error_code ec; std::filesystem::create_directories(item->path.parent_path(), ec);
  if (ec) { if (error != nullptr) *error = ec.message(); return nullptr; }
  item->file.open(item->path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
  if (!item->file) { if (error != nullptr) *error = "cannot open WAL"; return nullptr; }
  auto* result = item.get(); partitions_.push_back(std::move(item));
  if (!Recover(result, error)) { partitions_.pop_back(); return nullptr; }
  return result;
}

bool StorageEngine::Recover(Partition* partition, std::string* error) const {
  std::ifstream input(partition->path, std::ios::binary);
  std::uint64_t valid_bytes = 0;
  while (input) {
    char header[kRecordHeaderSize]; input.read(header, sizeof(header));
    if (input.gcount() == 0) break;
    if (input.gcount() != sizeof(header)) break;
    std::string h(header, sizeof(header)); const auto crc = Get32(h, 0); const auto len = Get32(h, 4);
    if (len < kBodyFixedSize || len > kBodyFixedSize + 65535 + kMaxMessageBytes) break;
    std::string body(len, '\0'); input.read(body.data(), static_cast<std::streamsize>(len));
    if (input.gcount() != static_cast<std::streamsize>(len) || Crc32(body) != crc) break;
    const auto key_len = Get16(body, 16); const auto value_len = Get32(body, 18 + key_len);
    if (22ull + key_len + value_len != len) break;
    Message message; message.offset = Get64(body, 0); message.timestamp_ms = static_cast<std::int64_t>(Get64(body, 8));
    message.key.assign(body, 18, key_len); message.value.assign(body, 22 + key_len, value_len);
    if (!partition->messages.empty() && message.offset != partition->messages.back().offset + 1) break;
    partition->messages.push_back(std::move(message)); valid_bytes += kRecordHeaderSize + len;
  }
  std::error_code ec; std::filesystem::resize_file(partition->path, valid_bytes, ec);
  if (ec) { if (error != nullptr) *error = ec.message(); return false; }
  return true;
}

bool StorageEngine::Append(const std::string& topic, std::uint32_t partition, std::string key,
                           std::string value, Message* message, std::string* error) {
  if (message == nullptr || key.size() > 65535 || value.empty() || value.size() > kMaxMessageBytes) {
    if (error != nullptr) *error = "invalid message";
    return false;
  }
  std::lock_guard lock(mutex_);
  if (!opened_) {
    std::error_code ec;
    std::filesystem::create_directories(data_dir_ / "queues", ec);
    if (ec) { if (error != nullptr) *error = ec.message(); return false; }
    opened_ = true;
  }
  auto* target = GetPartition(topic, partition, error); if (target == nullptr) return false;
  Message next; next.offset = target->messages.size(); next.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count(); next.key = std::move(key); next.value = std::move(value);
  std::string body; Put64(&body, next.offset); Put64(&body, static_cast<std::uint64_t>(next.timestamp_ms));
  Put16(&body, static_cast<std::uint16_t>(next.key.size())); body.append(next.key); Put32(&body, static_cast<std::uint32_t>(next.value.size())); body.append(next.value);
  std::string record; Put32(&record, Crc32(body)); Put32(&record, static_cast<std::uint32_t>(body.size())); record.append(body);
  target->file.clear(); target->file.seekp(0, std::ios::end); target->file.write(record.data(), static_cast<std::streamsize>(record.size()));
  if (!target->file) { if (error != nullptr) *error = "WAL write failed"; return false; }
  target->messages.push_back(next); *message = std::move(next); return true;
}

bool StorageEngine::Read(const std::string& topic, std::uint32_t partition, std::uint64_t start_offset,
                         std::uint32_t max_bytes, std::vector<Message>* messages, std::string* error) const {
  if (messages == nullptr || max_bytes == 0) return false;
  std::lock_guard lock(mutex_);
  auto* target = GetPartition(topic, partition, error); if (target == nullptr) return false;
  messages->clear(); std::uint64_t bytes = 0;
  for (const auto& item : target->messages) {
    if (item.offset < start_offset) continue;
    const auto size = item.key.size() + item.value.size(); if (!messages->empty() && bytes + size > max_bytes) break;
    if (size > max_bytes) break;
    messages->push_back(item);
    bytes += size;
  }
  return true;
}

bool StorageEngine::Flush(std::string* error) {
  std::lock_guard lock(mutex_); for (auto& item : partitions_) { item->file.flush(); if (!item->file && error != nullptr) *error = "WAL flush failed"; }
  return error == nullptr || error->empty();
}

}  // namespace mq::core
