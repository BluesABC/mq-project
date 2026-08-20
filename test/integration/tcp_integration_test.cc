#include <atomic>
#include <cassert>
#include <string>
#include <thread>
#include <vector>

#include <winsock2.h>
#include <ws2tcpip.h>

#include "mq/network/tcp_server.h"
#include "mq/protocol/protocol_codec.h"

namespace {

void Client(std::uint16_t port, std::uint64_t id, std::atomic<std::size_t>* completed) {
  SOCKET socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  assert(socket != INVALID_SOCKET);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  assert(connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
  mq::protocol::Request request;
  request.command = mq::protocol::Command::kHeartbeat;
  request.request_id = id;
  std::string frame;
  assert(mq::protocol::ProtocolCodec::EncodeRequest(request, &frame));
  assert(send(socket, frame.data(), static_cast<int>(frame.size()), 0) == static_cast<int>(frame.size()));
  std::string response(18, '\0');
  std::size_t received = 0;
  while (received < response.size()) {
    const int count = recv(socket, &response[received], static_cast<int>(response.size() - received), 0);
    assert(count > 0);
    received += count;
  }
  mq::protocol::Response decoded;
  assert(mq::protocol::ProtocolCodec::DecodeResponse(response, &decoded));
  assert(decoded.status == mq::protocol::Status::kOk && decoded.request_id == id);
  closesocket(socket);
  completed->fetch_add(1, std::memory_order_relaxed);
}

}  // namespace

int main() {
  mq::network::TcpServer server(0, 4, [](const mq::protocol::Request& request) {
    mq::protocol::Response response;
    response.request_id = request.request_id;
    response.status = mq::protocol::Status::kOk;
    return response;
  });
  assert(server.Start());
  while (server.port() == 0) std::this_thread::yield();
  std::atomic<std::size_t> completed{0};
  std::vector<std::thread> clients;
  for (std::uint64_t id = 1; id <= 100; ++id) clients.emplace_back(Client, server.port(), id, &completed);
  for (auto& client : clients) client.join();
  server.Stop();
  assert(completed.load(std::memory_order_relaxed) == 100);
  return 0;
}
