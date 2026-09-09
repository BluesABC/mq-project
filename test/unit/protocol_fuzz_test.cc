/**
 * @file protocol_fuzz_test.cc
 * @brief 协议模糊测试(Fuzz Test)
 * 
 * 本文件包含对协议编解码器的模糊测试，验证以下功能：
 * 1. 随机数据不会导致崩溃（鲁棒性测试）
 * 2. 边界长度（超大payload）被正确拒绝
 * 3. 流式解码器处理随机分片数据的正确性
 * 
 * 测试使用随机数生成器模拟各种异常输入场景。
 */

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "mq/protocol/protocol_codec.h"

namespace {

/**
 * @brief 辅助函数：在字符串指定位置设置32位大端整数
 * @param value 目标字符串
 * @param position 写入位置
 * @param number 要写入的32位整数
 */
void Set32(std::string* value, std::size_t position, std::uint32_t number) {
  for (int shift = 24; shift >= 0; shift -= 8)
    (*value)[position++] = static_cast<char>(number >> shift);
}

/**
 * @brief 验证解码后的请求是否符合约束
 * @param request 要验证的请求
 */
void CheckRequest(const mq::protocol::Request& request) {
  assert(request.topic.size() <= 65535);
  assert(request.payload.size() <= mq::protocol::kMaxPayloadBytes);
}

/**
 * @brief 验证解码后的响应是否符合约束
 * @param response 要验证的响应
 */
void CheckResponse(const mq::protocol::Response& response) {
  assert(response.payload.size() <= mq::protocol::kMaxPayloadBytes);
}

/**
 * @brief 测试随机数据不会导致解码器崩溃
 */
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

/**
 * @brief 测试边界长度（超大payload）被正确拒绝
 */
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

/**
 * @brief 测试流式解码器处理随机分片数据的正确性
 */
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

/**
 * @brief 主函数，运行所有协议模糊测试
 * @return 0 表示测试成功
 */
int main() {
  RandomFramesDoNotCrash();
  BoundaryLengthsAreRejected();
  StreamDecoderHandlesRandomChunks();
  return 0;
}
