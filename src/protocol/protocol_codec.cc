#include "mq/protocol/protocol_codec.h"

#include <limits>

namespace mq::protocol {
namespace {

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
bool Take(std::string_view in, std::size_t* pos, std::size_t count, std::string* error) {
  if (count > in.size() - *pos) {
    if (error != nullptr) *error = "frame is truncated";
    return false;
  }
  return true;
}
std::uint16_t Get16(std::string_view in, std::size_t pos) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(in[pos])) << 8) |
         static_cast<unsigned char>(in[pos + 1]);
}
std::uint32_t Get32(std::string_view in, std::size_t pos) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(in[pos + i]);
  return value;
}
std::uint64_t Get64(std::string_view in, std::size_t pos) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(in[pos + i]);
  return value;
}
bool ValidCommon(std::string_view frame, std::size_t header, std::size_t payload_offset,
                 std::uint8_t version, std::uint32_t payload_len, std::string* error) {
  if (frame.size() < header || Get16(frame, 0) != kMagic) {
    if (error != nullptr) *error = "invalid magic or header";
    return false;
  }
  if (version != kCurrentVersion) {
    if (error != nullptr) *error = "unsupported protocol version";
    return false;
  }
  if (payload_len > kMaxPayloadBytes || payload_len != frame.size() - payload_offset) {
    if (error != nullptr) *error = "invalid payload length";
    return false;
  }
  return true;
}
bool CheckString(const std::string& value, std::size_t limit, std::string* error) {
  if (value.size() > limit) {
    if (error != nullptr) *error = "field is too large";
    return false;
  }
  return true;
}

}  // namespace

bool ProtocolCodec::EncodeRequest(const Request& request, std::string* frame, std::string* error) {
  if (frame == nullptr || request.version != kCurrentVersion || !CheckString(request.topic, 65535, error) ||
      !CheckString(request.payload, kMaxPayloadBytes, error)) return false;
  frame->clear();
  Put16(frame, kMagic); frame->push_back(static_cast<char>(request.version));
  frame->push_back(static_cast<char>(request.command)); Put64(frame, request.request_id);
  Put16(frame, request.flags); Put16(frame, static_cast<std::uint16_t>(request.topic.size()));
  frame->append(request.topic); Put32(frame, static_cast<std::uint32_t>(request.payload.size()));
  frame->append(request.payload); return true;
}

bool ProtocolCodec::EncodeResponse(const Response& response, std::string* frame, std::string* error) {
  if (frame == nullptr || response.version != kCurrentVersion || !CheckString(response.payload, kMaxPayloadBytes, error)) return false;
  frame->clear(); Put16(frame, kMagic); frame->push_back(static_cast<char>(response.version));
  frame->push_back(static_cast<char>(response.status)); Put64(frame, response.request_id);
  Put16(frame, response.flags); Put32(frame, static_cast<std::uint32_t>(response.payload.size()));
  frame->append(response.payload); return true;
}

bool ProtocolCodec::DecodeRequest(std::string_view frame, Request* request, std::string* error) {
  if (request == nullptr || frame.size() < 20) return false;
  const auto topic_len = Get16(frame, 14);
  if (!Take(frame, &const_cast<std::size_t&>(static_cast<const std::size_t&>(20)), 0, error)) return false;
  const std::size_t payload_offset = 20 + topic_len;
  if (payload_offset > frame.size() || payload_offset + 4 > frame.size()) return false;
  if (!ValidCommon(frame, 20 + topic_len + 4, payload_offset + 4, static_cast<std::uint8_t>(frame[2]),
                   Get32(frame, payload_offset), error)) return false;
  request->version = static_cast<std::uint8_t>(frame[2]); request->command = static_cast<Command>(frame[3]);
  request->request_id = Get64(frame, 4); request->flags = Get16(frame, 12);
  request->topic.assign(frame.substr(16, topic_len)); request->payload.assign(frame.substr(payload_offset + 4));
  return true;
}

bool ProtocolCodec::DecodeResponse(std::string_view frame, Response* response, std::string* error) {
  if (response == nullptr || frame.size() < 18) return false;
  const auto payload_len = Get32(frame, 14);
  if (!ValidCommon(frame, 18, 18, static_cast<std::uint8_t>(frame[2]), payload_len, error)) return false;
  response->version = static_cast<std::uint8_t>(frame[2]); response->status = static_cast<Status>(frame[3]);
  response->request_id = Get64(frame, 4); response->flags = Get16(frame, 12);
  response->payload.assign(frame.substr(18)); return true;
}

}  // namespace mq::protocol
