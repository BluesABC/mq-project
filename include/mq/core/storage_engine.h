#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <memory>
#include <string>
#include <vector>

namespace mq::core {

struct Message {
  std::uint64_t offset = 0;
  std::int64_t timestamp_ms = 0;
  std::string key;
  std::string value;
};

class StorageEngine {
 public:
  explicit StorageEngine(std::filesystem::path data_dir);
  ~StorageEngine();

  StorageEngine(const StorageEngine&) = delete;
  StorageEngine& operator=(const StorageEngine&) = delete;

  bool Open(std::string* error = nullptr);
  bool Append(const std::string& topic, std::uint32_t partition, std::string key,
              std::string value, Message* message, std::string* error = nullptr);
  bool Read(const std::string& topic, std::uint32_t partition, std::uint64_t start_offset,
            std::uint32_t max_bytes, std::vector<Message>* messages,
            std::string* error = nullptr) const;
  bool Flush(std::string* error = nullptr);

 private:
  struct Partition;
  Partition* GetPartition(const std::string& topic, std::uint32_t partition,
                          std::string* error) const;
  bool Recover(Partition* partition, std::string* error);

  std::filesystem::path data_dir_;
  mutable std::mutex mutex_;
  mutable std::vector<std::unique_ptr<Partition>> partitions_;
  bool opened_ = false;
};

}  // namespace mq::core
