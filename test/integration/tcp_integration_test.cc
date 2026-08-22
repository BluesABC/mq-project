#include <atomic>
#include <cassert>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
using Socket = SOCKET;
constexpr Socket kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>
using Socket = int;
constexpr Socket kInvalidSocket = -1;
#endif

#include "mq/network/tcp_server.h"
#include "mq/protocol/protocol_codec.h"

namespace {
bool RaiseFileDescriptorLimit() {
#ifndef _WIN32
  rlimit limit{};
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0) return false;
  if (limit.rlim_cur >= 4096) return true;
  if (limit.rlim_max < 4096) return false;
  limit.rlim_cur = limit.rlim_max;
  return setrlimit(RLIMIT_NOFILE, &limit) == 0;
#else
  return true;
#endif
}

void CloseSocket(Socket socket) {
#ifdef _WIN32
  closesocket(socket);
#else
  close(socket);
#endif
}
void Client(std::uint16_t port, std::uint64_t id, std::atomic<std::size_t>* completed) {
  Socket socket = ::socket(AF_INET, SOCK_STREAM, 0); assert(socket != kInvalidSocket);
  sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK); address.sin_port = htons(port);
  assert(connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
  mq::protocol::Request request; request.command = mq::protocol::Command::kHeartbeat; request.request_id = id;
  std::string frame; assert(mq::protocol::ProtocolCodec::EncodeRequest(request, &frame));
  assert(send(socket, frame.data(), static_cast<int>(frame.size()), 0) == static_cast<int>(frame.size()));
  std::string response(18, '\0'); std::size_t received = 0;
  while (received < response.size()) { const auto count = recv(socket, response.data() + received, response.size() - received, 0); assert(count > 0); received += static_cast<std::size_t>(count); }
  mq::protocol::Response decoded; assert(mq::protocol::ProtocolCodec::DecodeResponse(response, &decoded));
  assert(decoded.status == mq::protocol::Status::kOk && decoded.request_id == id);
  CloseSocket(socket); completed->fetch_add(1, std::memory_order_relaxed);
}
}  // namespace

int main() {
  assert(RaiseFileDescriptorLimit());
  mq::network::TcpServer server(0, 0, [](const mq::protocol::Request& request) {
    mq::protocol::Response response; response.request_id = request.request_id; response.status = mq::protocol::Status::kOk; return response;
  });
  assert(server.Start()); while (server.port() == 0) std::this_thread::yield();
  std::atomic<std::size_t> completed{0}; std::vector<std::thread> clients; clients.reserve(1000);
  for (std::uint64_t id = 1; id <= 1000; ++id) clients.emplace_back(Client, server.port(), id, &completed);
  for (auto& client : clients) client.join();
  server.Stop(); assert(completed.load(std::memory_order_relaxed) == 1000); return 0;
}
