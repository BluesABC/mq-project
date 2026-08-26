#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace mq::core {

// 消费位点以 group/topic/partition 为键持久化，重启后可恢复消费进度。
struct ConsumerOffset {
  std::string group;
  std::string topic;
  std::uint32_t partition = 0;
  std::uint64_t offset = 0;
};

class ConsumerOffsetStore {
 public:
  explicit ConsumerOffsetStore(std::filesystem::path path) : path_(std::move(path)) {}
  bool Save(const std::vector<ConsumerOffset>& offsets, std::string* error = nullptr) const;
  bool Load(std::vector<ConsumerOffset>* offsets, std::string* error = nullptr) const;

 private:
  std::filesystem::path path_;
};

}  // namespace mq::core
