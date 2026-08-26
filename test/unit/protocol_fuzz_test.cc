#include <algorithm>
#include <cassert>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "mq/protocol/protocol_codec.h"

namespace {

void Set32(std::string* value, std::size_t position, std::uint32_t number) {
  for (int shift = 24; shift >= 0; shift -= 8)
    (*value)[position++] = static_cast<char>(number >> shift);
}

void CheckRequest(const mq::protocol::Request& request) {
  assert(request.topic.size() <= 65535);
  assert(request.payload.size() <= mq::protocol::kMaxPayloadBytes);
}

void CheckResponse(const mq::protocol::Response& response) {
  assert(response.payload.size() <= mq::protocol::kMaxPayloadBytes);
}

void RandomFramesDoNotCrash() {
  std::mt19937 generator(0x4D515F46);
  std::uniform_int_distribution<std::size_t> size_distribution(0, 4096);
  std::uniform_int_distribution<unsigned int> byte_distribution(0, 255);

  for (std::size_t iteration = 0; iteration < 20000; ++iteration) {
    std::string input(size_distribution(generator), '\0');
    for (char& byte : input) byte = static_cast<char>(byte_distribution(generator));

    std::string error;
    mq::protocol::Request request;
    if (mq::protocol::ProtocolCodec::DecodeRequest(input, &request, &error)) CheckRequest(request);
    mq::protocol::Response response;
    if (mq::protocol::ProtocolCodec::DecodeResponse(input, &response, &error))
      CheckResponse(response);
  }
}

void BoundaryLengthsAreRejected() {
  std::string oversized_request(20, '\0');
  oversized_request[0] = static_cast<char>(mq::protocol::kMagic >> 8);
  oversized_request[1] = static_cast<char>(mq::protocol::kMagic);
  Set32(&oversized_request, 16, mq::protocol::kMaxPayloadBytes + 1);
  mq::protocol::Request request;
  assert(!mq::protocol::ProtocolCodec::DecodeRequest(oversized_request, &request));

  std::string oversized_response(18, '\0');
  oversized_response[0] = static_cast<char>(mq::protocol::kMagic >> 8);
  oversized_response[1] = static_cast<char>(mq::protocol::kMagic);
  Set32(&oversized_response, 14, mq::protocol::kMaxPayloadBytes + 1);
  mq::protocol::Response response;
  assert(!mq::protocol::ProtocolCodec::DecodeResponse(oversized_response, &response));
}

void StreamDecoderHandlesRandomChunks() {
  mq::protocol::Request first;
  first.command = mq::protocol::Command::kHeartbeat;
  first.request_id = 1;
  first.topic = "orders";
  mq::protocol::Request second = first;
  second.command = mq::protocol::Command::kProduce;
  second.request_id = 2;
  second.payload = "payload";
  std::string first_frame;
  std::string second_frame;
  std::string error;
  assert(mq::protocol::ProtocolCodec::EncodeRequest(first, &first_frame, &error));
  assert(mq::protocol::ProtocolCodec::EncodeRequest(second, &second_frame, &error));
  const std::string stream = first_frame + second_frame;

  std::mt19937 generator(0x5354524D);
  std::uniform_int_distribution<std::size_t> chunk_distribution(1, 17);
  for (std::size_t iteration = 0; iteration < 1000; ++iteration) {
    mq::protocol::RequestStreamDecoder decoder;
    std::vector<mq::protocol::Request> requests;
    std::size_t position = 0;
    while (position < stream.size()) {
      const std::size_t chunk =
          std::min(chunk_distribution(generator), stream.size() - position);
      assert(decoder.Push(std::string_view(stream).substr(position, chunk), &requests, &error));
      position += chunk;
    }
    assert(requests.size() == 2);
    CheckRequest(requests[0]);
    CheckRequest(requests[1]);
    assert(requests[0].request_id == 1 && requests[1].request_id == 2);
  }
}

}  // namespace

int main() {
  RandomFramesDoNotCrash();
  BoundaryLengthsAreRejected();
  StreamDecoderHandlesRandomChunks();
  return 0;
}
