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

namespace mq::core { namespace {
constexpr std::uint32_t kVersion = 1;
constexpr std::uint32_t kMaxTopics = 1024;

void Put16(std::ostream& out, std::uint16_t value) {
  out.put(static_cast<char>(value >> 8));
  out.put(static_cast<char>(value));
}

void Put32(std::ostream& out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out.put(static_cast<char>(value >> shift));
  }
}

bool Get16(std::istream& in, std::uint16_t* value) {
  const int first = in.get();
  const int second = in.get();
  if (first < 0 || second < 0) return false;
  *value = static_cast<std::uint16_t>((first << 8) | second);
  return true;
}

bool Get32(std::istream& in, std::uint32_t* value) {
  *value = 0;
  for (int index = 0; index < 4; ++index) {
    const int byte = in.get();
    if (byte < 0) return false;
    *value = (*value << 8) | static_cast<std::uint32_t>(byte);
  }
  return true;
}

void SetError(std::string* error, std::string message) {
  if (error != nullptr) *error = std::move(message);
}

}  // namespace

bool TopicMetadataStore::Save(const std::vector<TopicMetadata>& topics, std::string* error) const {
  if (topics.size() > kMaxTopics) {
    SetError(error, "too many topics in metadata");
    return false;
  }
  for (const auto& topic : topics) {
    if (!IsValidTopicName(topic.name) || topic.partition_count == 0 ||
        topic.name.size() > (std::numeric_limits<std::uint16_t>::max)()) {
      SetError(error, "invalid topic metadata");
      return false;
    }
  }

  std::error_code filesystem_error;
  std::filesystem::create_directories(path_.parent_path(), filesystem_error);
  if (filesystem_error) {
    SetError(error, filesystem_error.message());
    return false;
  }

  auto temporary = path_;
  temporary += ".tmp";
  std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
  if (!out) {
    SetError(error, "cannot open topic metadata temporary file");
    return false;
  }
  out.write("MQTM", 4);
  Put32(out, kVersion);
  Put32(out, static_cast<std::uint32_t>(topics.size()));
  for (const auto& topic : topics) {
    Put16(out, static_cast<std::uint16_t>(topic.name.size()));
    out.write(topic.name.data(), static_cast<std::streamsize>(topic.name.size()));
    Put32(out, topic.partition_count);
  }
  out.flush();
  out.close();
  if (!out) {
    std::filesystem::remove(temporary, filesystem_error);
    SetError(error, "cannot write topic metadata");
    return false;
  }

#ifdef _WIN32
  if (!MoveFileExW(temporary.c_str(), path_.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
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

bool TopicMetadataStore::Load(std::vector<TopicMetadata>* topics, std::string* error) const {
  if (topics == nullptr) {
    SetError(error, "topic output is null");
    return false;
  }
  topics->clear();
  std::error_code filesystem_error;
  if (!std::filesystem::exists(path_, filesystem_error)) {
    if (filesystem_error) SetError(error, filesystem_error.message());
    return !filesystem_error;
  }

  std::ifstream in(path_, std::ios::binary);
  char magic[4];
  std::uint32_t version = 0;
  std::uint32_t count = 0;
  if (!in.read(magic, 4) || std::string(magic, 4) != "MQTM" || !Get32(in, &version) ||
      version != kVersion || !Get32(in, &count) || count > kMaxTopics) {
    SetError(error, "invalid topic metadata");
    return false;
  }

  topics->reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    std::uint16_t name_length = 0;
    std::uint32_t partition_count = 0;
    if (!Get16(in, &name_length)) {
      SetError(error, "truncated topic metadata");
      return false;
    }
    TopicMetadata topic;
    topic.name.resize(name_length);
    if (!in.read(topic.name.data(), name_length) || !Get32(in, &partition_count)) {
      SetError(error, "truncated topic metadata");
      return false;
    }
    topic.partition_count = partition_count;
    if (!IsValidTopicName(topic.name) || topic.partition_count == 0) {
      SetError(error, "invalid topic metadata");
      return false;
    }
    topics->push_back(std::move(topic));
  }
  return true;
}

}  // namespace mq::core
