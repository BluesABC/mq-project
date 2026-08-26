#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "mq/protocol/commands.h"

namespace mq::protocol {

// 负责协议帧的边界校验和编解码；网络层无需了解字段的二进制布局。
class ProtocolCodec {
 public:
  static bool EncodeRequest(const Request& request, std::string* frame,
                            std::string* error = nullptr);
  static bool EncodeResponse(const Response& response, std::string* frame,
                             std::string* error = nullptr);
  static bool DecodeRequest(std::string_view frame, Request* request, std::string* error = nullptr);
  static bool DecodeResponse(std::string_view frame, Response* response,
                             std::string* error = nullptr);
};

class RequestStreamDecoder {
 public:
  // 一次 Push 可能包含半帧或多个帧，内部缓冲用于处理 TCP 的半包/粘包。
  bool Push(std::string_view bytes, std::vector<Request>* requests, std::string* error = nullptr);

 private:
  std::string buffer_;
};

}  // namespace mq::protocol
