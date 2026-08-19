#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "mq/protocol/commands.h"

namespace mq::protocol {

class ProtocolCodec {
 public:
  static bool EncodeRequest(const Request& request, std::string* frame,
                            std::string* error = nullptr);
  static bool EncodeResponse(const Response& response, std::string* frame,
                             std::string* error = nullptr);
  static bool DecodeRequest(std::string_view frame, Request* request,
                            std::string* error = nullptr);
  static bool DecodeResponse(std::string_view frame, Response* response,
                             std::string* error = nullptr);
};

}  // namespace mq::protocol
