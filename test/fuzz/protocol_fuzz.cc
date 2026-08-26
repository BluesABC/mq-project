#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "mq/protocol/protocol_codec.h"

// libFuzzer 通过该入口持续变异协议帧，解析失败是正常输入，不应导致进程崩溃。
namespace {

void Put16(std::string* output, std::uint16_t value) {
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value));
}

void Put32(std::string* output, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8)
    output->push_back(static_cast<char>(value >> shift));
}

void Put64(std::string* output, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8)
    output->push_back(static_cast<char>(value >> shift));
}

std::uint8_t ByteAt(const std::uint8_t* data, std::size_t size, std::size_t position) {
  return size == 0 ? 0 : data[position % size];
}

std::string MakeGuidedRequest(const std::uint8_t* data, std::size_t size) {
  mq::protocol::Request request;
  request.request_id = 1;
  request.topic = "fuzz-topic";
  switch (ByteAt(data, size, 0) % 5) {
    case 0:
      request.command = mq::protocol::Command::kHeartbeat;
      break;
    case 1:
      request.command = mq::protocol::Command::kCreateTopic;
      Put32(&request.payload, 1 + ByteAt(data, size, 1) % 4);
      break;
    case 2: {
      request.command = mq::protocol::Command::kProduce;
      Put32(&request.payload, 0xFFFFFFFFu);
      Put16(&request.payload, 1);
      request.payload.push_back(static_cast<char>(ByteAt(data, size, 1)));
      const auto value_size = std::min<std::size_t>(32, size);
      Put32(&request.payload, static_cast<std::uint32_t>(value_size == 0 ? 1 : value_size));
      if (value_size == 0)
        request.payload.push_back('v');
      else
        request.payload.append(reinterpret_cast<const char*>(data), value_size);
      break;
    }
    case 3:
      request.command = mq::protocol::Command::kFetch;
      Put32(&request.payload, 0);
      Put64(&request.payload, ByteAt(data, size, 1));
      Put32(&request.payload, 1024);
      break;
    default:
      request.command = mq::protocol::Command::kCommitOffset;
      Put16(&request.payload, 5);
      request.payload.append("group");
      Put32(&request.payload, 0);
      Put64(&request.payload, ByteAt(data, size, 1));
      break;
  }
  std::string frame;
  std::string error;
  mq::protocol::ProtocolCodec::EncodeRequest(request, &frame, &error);
  return frame;
}

void ExerciseGuidedFrames(const std::uint8_t* data, std::size_t size) {
  std::string error;
  const auto request_frame = MakeGuidedRequest(data, size);
  mq::protocol::Request request;
  mq::protocol::ProtocolCodec::DecodeRequest(request_frame, &request, &error);
  mq::protocol::RequestStreamDecoder decoder;
  std::vector<mq::protocol::Request> requests;
  decoder.Push(request_frame, &requests, &error);

  mq::protocol::Response response;
  response.request_id = request.request_id;
  response.status = mq::protocol::Status::kOk;
  response.payload = request.payload;
  std::string response_frame;
  mq::protocol::ProtocolCodec::EncodeResponse(response, &response_frame, &error);
  mq::protocol::Response decoded_response;
  mq::protocol::ProtocolCodec::DecodeResponse(response_frame, &decoded_response, &error);
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (data == nullptr) return 0;
  const std::string_view input(reinterpret_cast<const char*>(data), size);
  std::string error;
  mq::protocol::Request request;
  mq::protocol::ProtocolCodec::DecodeRequest(input, &request, &error);
  mq::protocol::Response response;
  mq::protocol::ProtocolCodec::DecodeResponse(input, &response, &error);
  mq::protocol::RequestStreamDecoder decoder;
  std::vector<mq::protocol::Request> requests;
  decoder.Push(input, &requests, &error);
  ExerciseGuidedFrames(data, size);
  return 0;
}
