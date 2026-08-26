#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "mq/network/event_loop.h"
#include "mq/network/tls.h"
#include "mq/protocol/commands.h"

namespace mq::network {

// TcpServer 负责监听和连接分发，业务请求通过 RequestHandler 回调交给上层 Broker。
class TcpServer {
 public:
  using RequestHandler = std::function<protocol::Response(const protocol::Request&)>;
  TcpServer(std::uint16_t port, std::size_t worker_count, RequestHandler handler,
            TlsOptions tls_options = {});
  TcpServer(std::string bind_address, std::uint16_t port, std::size_t worker_count,
            RequestHandler handler, TlsOptions tls_options = {});
  ~TcpServer();
  bool Start();
  void Stop();
  std::uint16_t port() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace mq::network
