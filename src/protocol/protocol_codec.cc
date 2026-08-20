#include "mq/protocol/protocol_codec.h"

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
  if (request == nullptr || frame.size() < 20) {
    if (error != nullptr) *error = "request header is truncated";
    return false;
  }
  const auto topic_len = Get16(frame, 14);
  const std::size_t payload_length_offset = 16 + topic_len;
  const std::size_t payload_offset = payload_length_offset + 4;
  if (payload_offset > frame.size()) {
    if (error != nullptr) *error = "request topic header is truncated";
    return false;
  }
  if (!ValidCommon(frame, payload_offset, payload_offset, static_cast<std::uint8_t>(frame[2]),
                   Get32(frame, payload_length_offset), error)) return false;
  request->version = static_cast<std::uint8_t>(frame[2]); request->command = static_cast<Command>(frame[3]);
  request->request_id = Get64(frame, 4); request->flags = Get16(frame, 12);
  request->topic.assign(frame.substr(16, topic_len));
  request->payload.assign(frame.substr(payload_offset));
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

bool RequestStreamDecoder::Push(std::string_view bytes, std::vector<Request>* requests,
                                std::string* error) {
  if (requests == nullptr || bytes.size() > kMaxPayloadBytes + 65555 ||
      buffer_.size() > kMaxPayloadBytes + 65555 - bytes.size()) {
    if (error != nullptr) *error = "request stream buffer exceeds limit";
    buffer_.clear();
    return false;
  }
  buffer_.append(bytes.data(), bytes.size());
  while (true) {
    if (buffer_.size() < 20) return true;
    if (Get16(buffer_, 0) != kMagic || static_cast<std::uint8_t>(buffer_[2]) != kCurrentVersion) {
      if (error != nullptr) *error = "invalid request stream header";
      buffer_.clear();
      return false;
    }
    const std::size_t topic_len = Get16(buffer_, 14);
    const std::size_t payload_length_offset = 16 + topic_len;
    if (payload_length_offset + 4 > buffer_.size()) return true;
    const std::size_t payload_len = Get32(buffer_, payload_length_offset);
    if (payload_len > kMaxPayloadBytes ||
        payload_len > kMaxPayloadBytes + 65555 - (payload_length_offset + 4)) {
      if (error != nullptr) *error = "invalid request stream payload length";
      buffer_.clear();
      return false;
    }
    const std::size_t frame_size = payload_length_offset + 4 + payload_len;
    if (buffer_.size() < frame_size) return true;
    Request request;
    if (!ProtocolCodec::DecodeRequest(std::string_view(buffer_).substr(0, frame_size), &request,
                                      error)) {
      buffer_.clear();
      return false;
    }
    requests->push_back(std::move(request));
    buffer_.erase(0, frame_size);
  }
}

}  // namespace mq::protocol
