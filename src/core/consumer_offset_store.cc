#include "mq/core/consumer_offset_store.h"

#include <fstream>
#include <limits>
#ifdef _WIN32
#include <windows.h>
#endif

namespace mq::core {
namespace {
void Put16(std::ostream& out, std::uint16_t value) {
  out.put(static_cast<char>(value >> 8));
  out.put(static_cast<char>(value));
}
void Put32(std::ostream& out, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) out.put(static_cast<char>(value >> shift));
}
void Put64(std::ostream& out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) out.put(static_cast<char>(value >> shift));
}
bool Get16(std::istream& in, std::uint16_t* value) {
  int a = in.get(), b = in.get();
  if (a < 0 || b < 0) return false;
  *value = static_cast<std::uint16_t>((a << 8) | b);
  return true;
}
bool Get32(std::istream& in, std::uint32_t* value) {
  *value = 0;
  for (int i = 0; i < 4; ++i) {
    int b = in.get();
    if (b < 0) return false;
    *value = (*value << 8) | static_cast<std::uint32_t>(b);
  }
  return true;
}
bool Get64(std::istream& in, std::uint64_t* value) {
  *value = 0;
  for (int i = 0; i < 8; ++i) {
    int b = in.get();
    if (b < 0) return false;
    *value = (*value << 8) | static_cast<std::uint64_t>(b);
  }
  return true;
}
void Error(std::string* error, const char* message) {
  if (error) *error = message;
}
}  // namespace

bool ConsumerOffsetStore::Save(const std::vector<ConsumerOffset>& offsets,
                               std::string* error) const {
  if (offsets.size() > 100000) {
    Error(error, "too many consumer offsets");
    return false;
  }
  std::error_code ec;
  std::filesystem::create_directories(path_.parent_path(), ec);
  if (ec) {
    if (error) *error = ec.message();
    return false;
  }
  auto temporary = path_;
  temporary += ".tmp";
  std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
  if (!out) {
    Error(error, "cannot open consumer offset temporary file");
    return false;
  }
  out.write("MQCO", 4);
  Put32(out, 1);
  Put32(out, static_cast<std::uint32_t>(offsets.size()));
  for (const auto& item : offsets) {
    if (item.group.size() > 65535 || item.topic.size() > 65535) {
      Error(error, "consumer offset name too long");
      return false;
    }
    Put16(out, static_cast<std::uint16_t>(item.group.size()));
    out.write(item.group.data(), item.group.size());
    Put16(out, static_cast<std::uint16_t>(item.topic.size()));
    out.write(item.topic.data(), item.topic.size());
    Put32(out, item.partition);
    Put64(out, item.offset);
  }
  out.flush();
  out.close();
  if (!out) {
    std::filesystem::remove(temporary, ec);
    Error(error, "cannot write consumer offsets");
    return false;
  }
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

bool ConsumerOffsetStore::Load(std::vector<ConsumerOffset>* offsets, std::string* error) const {
  if (!offsets) {
    Error(error, "consumer offset output is null");
    return false;
  }
  offsets->clear();
  std::error_code ec;
  if (!std::filesystem::exists(path_, ec)) return !ec;
  std::ifstream in(path_, std::ios::binary);
  char magic[4];
  std::uint32_t version = 0, count = 0;
  if (!in.read(magic, 4) || std::string(magic, 4) != "MQCO" || !Get32(in, &version) ||
      version != 1 || !Get32(in, &count) || count > 100000) {
    Error(error, "invalid consumer offsets");
    return false;
  }
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint16_t group_size = 0, topic_size = 0;
    ConsumerOffset item;
    if (!Get16(in, &group_size) || group_size > 65535) {
      Error(error, "truncated consumer offsets");
      return false;
    }
    item.group.resize(group_size);
    if (!in.read(item.group.data(), group_size) || !Get16(in, &topic_size)) {
      Error(error, "truncated consumer offsets");
      return false;
    }
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
