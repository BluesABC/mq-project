#include "mq/network/tcp_server.h"

#include <atomic>
#include <map>
#include <utility>
#include <vector>

#include "mq/core/memory_pool.h"
#include "mq/core/thread_pool.h"
#include "mq/network/tcp_connection.h"
#include "mq/protocol/protocol_codec.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace mq::network {

struct TcpServer::Impl {
  Impl(std::uint16_t configured_port, std::size_t worker_count, RequestHandler configured_handler)
      : requested_port(configured_port), handler(std::move(configured_handler)), workers(worker_count, 1024), loop(1024) {}
  std::uint16_t requested_port;
  std::atomic<std::uint16_t> bound_port{0};
  RequestHandler handler;
  core::ThreadPool workers;
  EventLoop loop;
  std::atomic<bool> started{false};
#ifdef _WIN32
  struct Client { std::shared_ptr<core::MemoryPool> pool; std::shared_ptr<TcpConnection> connection; };
  SOCKET listener = INVALID_SOCKET;
  std::map<SOCKET, Client> clients;
  std::uint64_t next_id = 1;

  void CloseClient(SOCKET socket) { closesocket(socket); clients.erase(socket); }
  void Poll() {
    fd_set reads, writes; FD_ZERO(&reads); FD_ZERO(&writes); FD_SET(listener, &reads);
    for (const auto& item : clients) { FD_SET(item.first, &reads); if (!item.second.connection->Writable().empty()) FD_SET(item.first, &writes); }
    timeval timeout{}; if (select(0, &reads, &writes, nullptr, &timeout) <= 0) return;
    if (FD_ISSET(listener, &reads)) for (;;) {
      SOCKET socket = accept(listener, nullptr, nullptr); if (socket == INVALID_SOCKET) break;
      u_long one = 1; if (ioctlsocket(socket, FIONBIO, &one) != 0) { closesocket(socket); continue; }
      Client client; client.pool = std::make_shared<core::MemoryPool>(128 * 1024);
      client.connection = std::make_shared<TcpConnection>(next_id++, &loop, client.pool.get(), 64 * 1024, 64 * 1024);
      clients.emplace(socket, std::move(client));
    }
    char input[8192]; std::vector<SOCKET> closed;
    for (auto& item : clients) {
      if (FD_ISSET(item.first, &reads)) {
        const int count = recv(item.first, input, sizeof(input), 0);
        if (count <= 0) { closed.push_back(item.first); continue; }
        std::vector<protocol::Request> requests; std::string error;
        if (!item.second.connection->DecodeRequests(std::string_view(input, count), &requests, &error)) { closed.push_back(item.first); continue; }
        for (const auto& request : requests) {
          const auto connection = item.second.connection;
          if (!workers.Submit([this, connection, request] { protocol::Response response = handler(request); std::string frame; if (protocol::ProtocolCodec::EncodeResponse(response, &frame)) connection->Send(std::move(frame)); })) closed.push_back(item.first);
        }
      }
      if (FD_ISSET(item.first, &writes)) {
        const std::string_view output = item.second.connection->Writable();
        const int count = send(item.first, output.data(), static_cast<int>(output.size()), 0);
        if (count > 0) item.second.connection->ConsumeWritten(static_cast<std::size_t>(count));
        else if (count == 0 || WSAGetLastError() != WSAEWOULDBLOCK) closed.push_back(item.first);
      }
    }
    for (SOCKET socket : closed) if (clients.find(socket) != clients.end()) CloseClient(socket);
  }
#endif
};

TcpServer::TcpServer(std::uint16_t port, std::size_t worker_count, RequestHandler handler)
    : impl_(std::make_unique<Impl>(port, worker_count, std::move(handler))) {}
TcpServer::~TcpServer() { Stop(); }

bool TcpServer::Start() {
#ifdef _WIN32
  if (!impl_->handler || impl_->started.exchange(true)) return false;
  WSADATA data{}; if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return false;
  impl_->listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); u_long one = 1;
  if (impl_->listener == INVALID_SOCKET || ioctlsocket(impl_->listener, FIONBIO, &one) != 0) { Stop(); return false; }
  sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK); address.sin_port = htons(impl_->requested_port);
  if (bind(impl_->listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || listen(impl_->listener, SOMAXCONN) != 0) { Stop(); return false; }
  int length = sizeof(address); getsockname(impl_->listener, reinterpret_cast<sockaddr*>(&address), &length); impl_->bound_port.store(ntohs(address.sin_port));
  impl_->loop.SetIdleCallback([this] { impl_->Poll(); });
  return impl_->loop.Start();
#else
  return false;
#endif
}

void TcpServer::Stop() {
#ifdef _WIN32
  if (!impl_ || !impl_->started.exchange(false)) return;
  impl_->workers.Shutdown();
  impl_->loop.Stop(); for (const auto& item : impl_->clients) closesocket(item.first); impl_->clients.clear();
  if (impl_->listener != INVALID_SOCKET) closesocket(impl_->listener); impl_->listener = INVALID_SOCKET; WSACleanup();
#endif
}

std::uint16_t TcpServer::port() const { return impl_->bound_port.load(); }
}  // namespace mq::network
