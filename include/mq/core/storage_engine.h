#pragma once

#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mq::core {

// fsync 策略在可靠性和吞吐之间取舍；配置为每批或按时间间隔时需要显式 Flush。
enum class FsyncPolicy { kPerMessage, kPerBatch, kInterval };

struct StorageConfig {
  std::uint64_t segment_size_bytes = 64ULL * 1024 * 1024;
  std::uint32_t index_interval = 1000;
  FsyncPolicy fsync_policy = FsyncPolicy::kPerBatch;
  std::uint32_t fsync_interval_ms = 5;
  std::uint64_t retention_ms = 7ULL * 24 * 60 * 60 * 1000;
  std::uint64_t retention_bytes = 1024ULL * 1024 * 1024;
  std::uint32_t cleaner_interval_ms = 1000;
};

struct Message {
  // offset 只在同一 Topic 分区内有序，不能跨分区比较或提交。
  std::uint64_t offset = 0;
  std::int64_t timestamp_ms = 0;
  std::string key;
  std::string value;
};

class StorageEngine {
 public:
  explicit StorageEngine(std::filesystem::path data_dir);
  StorageEngine(std::filesystem::path data_dir, StorageConfig config);
  ~StorageEngine();
  StorageEngine(const StorageEngine&) = delete;
  StorageEngine& operator=(const StorageEngine&) = delete;

  // Open 负责建立目录并启动清理线程；后续写入统一从本接口进入 WAL。
  bool Open(std::string* error = nullptr);
  bool Append(const std::string& topic, std::uint32_t partition, std::string key, std::string value,
              Message* message, std::string* error = nullptr);
  // 副本追加要求 offset 连续，借此发现复制缺口而不是静默覆盖数据。
  bool AppendReplica(const std::string& topic, std::uint32_t partition, const Message& message,
                     std::string* error = nullptr);
  // Read 以 max_bytes 限制单次返回量，防止消费者请求导致无界内存增长。
  bool Read(const std::string& topic, std::uint32_t partition, std::uint64_t start_offset,
            std::uint32_t max_bytes, std::vector<Message>* messages,
            std::string* error = nullptr) const;
  bool Flush(std::string* error = nullptr);
  bool CleanupExpiredSegments(std::string* error = nullptr);

 private:
  struct Partition;
  Partition* GetPartition(const std::string& topic, std::uint32_t partition,
                          std::string* error) const;
  bool Recover(Partition* partition, std::string* error) const;
  void CleanerLoop();

  std::filesystem::path data_dir_;
  StorageConfig config_;
  mutable std::mutex mutex_;
  mutable std::vector<std::unique_ptr<Partition>> partitions_;
  std::thread cleaner_thread_;
  std::condition_variable cleaner_cv_;
  bool stop_cleaner_ = false;
  bool opened_ = false;
};

}  // namespace mq::core
