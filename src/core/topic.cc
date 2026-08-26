#include "mq/core/topic.h"

#include <cstddef>

namespace mq::core {
namespace {

bool IsContinuation(unsigned char byte) {
  return (byte & 0xC0) == 0x80;
}

bool IsValidUtf8(std::string_view value) {
  for (std::size_t index = 0; index < value.size();) {
    const unsigned char first = static_cast<unsigned char>(value[index]);
    if (first < 0x80) {
      ++index;
      continue;
    }
    std::size_t length = 0;
    if (first >= 0xC2 && first <= 0xDF) {
      length = 2;
    } else if (first >= 0xE0 && first <= 0xEF) {
      length = 3;
    } else if (first >= 0xF0 && first <= 0xF4) {
      length = 4;
    } else {
      return false;
    }
    if (index + length > value.size()) return false;
    for (std::size_t offset = 1; offset < length; ++offset) {
      if (!IsContinuation(static_cast<unsigned char>(value[index + offset]))) return false;
    }
    const unsigned char second = static_cast<unsigned char>(value[index + 1]);
    if ((first == 0xE0 && second < 0xA0) || (first == 0xED && second >= 0xA0) ||
        (first == 0xF0 && second < 0x90) || (first == 0xF4 && second >= 0x90)) {
      return false;
    }
    index += length;
  }
  return true;
}

}  // namespace

bool IsValidTopicName(std::string_view topic) {
  if (topic.empty() || topic.size() > 64 || topic == "." || topic == "..") return false;
  for (const unsigned char byte : topic) {
    if (byte < 0x20 || byte == '/' || byte == '\\' || byte == ':' || byte == '*' || byte == '?' ||
        byte == '"' || byte == '<' || byte == '>' || byte == '|') {
      return false;
    }
  }
  return IsValidUtf8(topic);
}

}  // namespace mq::core
