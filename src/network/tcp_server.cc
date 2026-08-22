#include "mq/network/tcp_server.h"

#include <atomic>
#include <algorithm>
#include <map>
#include <thread>
#include <string>
#include <utility>
#include <vector>

#include "mq/core/memory_pool.h"
#include "mq/core/thread_pool.h"
#include "mq/network/tcp_connection.h"
#include "mq/protocol/protocol_codec.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace mq::network {

struct TcpServer::Impl {
  static std::size_t DefaultWorkers(std::size_t count) {
    constexpr std::size_t kMaxAutomaticWorkers = 32;
    const auto cores = std::thread::hardware_concurrency();
    if (count != 0) return count;
    if (cores == 0) return 1;
    return std::min<std::size_t>(cores, kMaxAutomaticWorkers);
  }
  Impl(std::uint16_t configured_port, std::size_t worker_count, RequestHandler configured_handler)
      : requested_port(configured_port), worker_count(DefaultWorkers(worker_count)), handler(std::move(configured_handler)), workers(DefaultWorkers(worker_count), 1024), loop(1024) {}
  Impl(std::string address, std::uint16_t configured_port, std::size_t count, RequestHandler configured_handler)
      : requested_address(std::move(address)), requested_port(configured_port), worker_count(DefaultWorkers(count)), handler(std::move(configured_handler)), workers(DefaultWorkers(count), 1024), loop(1024) {}
  std::string requested_address = "127.0.0.1";
  std::uint16_t requested_port;
  std::size_t worker_count;
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
#else
  struct Client { std::shared_ptr<core::MemoryPool> pool; std::shared_ptr<TcpConnection> connection; };
  struct SubReactor {
    explicit SubReactor(std::size_t capacity) : loop(capacity) {}
    EventLoop loop;
    std::map<int, Client> clients;
  };
  std::vector<std::unique_ptr<SubReactor>> subs;
  int listener = -1;
  std::size_t next_sub = 0;
  std::uint64_t next_id = 1;
  bool listener_registered = false;

  static bool NonBlocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
  }
  void CloseClient(SubReactor* sub, int fd) {
    const auto it = sub->clients.find(fd);
    if (it == sub->clients.end()) return;
    sub->loop.RemoveFd(fd);
    close(fd);
    sub->clients.erase(fd);
  }
  void HandleClient(SubReactor* sub, int fd, std::uint32_t events) {
    auto it = sub->clients.find(fd);
    if (it == sub->clients.end()) return;
    if ((events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) { CloseClient(sub, fd); return; }
    if ((events & EPOLLIN) != 0) {
      char buffer[64 * 1024];
      for (;;) {
        const ssize_t count = recv(fd, buffer, sizeof(buffer), 0);
        if (count > 0) {
          std::vector<protocol::Request> requests; std::string error;
          if (!it->second.connection->DecodeRequests(std::string_view(buffer, static_cast<std::size_t>(count)), &requests, &error)) { CloseClient(sub, fd); return; }
          for (const auto& request : requests) {
            const auto connection = it->second.connection;
            if (!workers.Submit([this, connection, request, sub, fd] {
                  protocol::Response response = handler(request); std::string frame;
                  if (protocol::ProtocolCodec::EncodeResponse(response, &frame) && connection->Send(std::move(frame))) {
                    sub->loop.QueueInLoop([sub, fd, connection] {
                      const auto current = sub->clients.find(fd);
                      if (current == sub->clients.end() || current->second.connection != connection) return;
                      sub->loop.ModifyFd(fd, EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP);
                    });
                  }
                })) { CloseClient(sub, fd); return; }
          }
          continue;
        }
        if (count == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) { CloseClient(sub, fd); return; }
        break;
      }
    }
    if ((events & EPOLLOUT) != 0) {
      while (!it->second.connection->Writable().empty()) {
        const auto output = it->second.connection->Writable();
        const ssize_t count = send(fd, output.data(), output.size(), MSG_NOSIGNAL);
        if (count > 0) { it->second.connection->ConsumeWritten(static_cast<std::size_t>(count)); continue; }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        CloseClient(sub, fd); return;
      }
    }
    sub->loop.ModifyFd(fd, EPOLLIN | EPOLLET | EPOLLRDHUP |
                            (it->second.connection->Writable().empty() ? 0 : EPOLLOUT));
  }
  void AddClient(SubReactor* sub, int fd) {
    auto pool = std::make_shared<core::MemoryPool>(128 * 1024);
    Client client{pool, std::make_shared<TcpConnection>(next_id++, &sub->loop, pool.get(), 64 * 1024, 64 * 1024)};
    const auto inserted = sub->clients.emplace(fd, std::move(client));
    if (!inserted.second) {
      close(fd);
      return;
    }
    if (!sub->loop.RegisterFd(fd, EPOLLIN | EPOLLET | EPOLLRDHUP, [this, sub, fd](std::uint32_t events) {
          try {
            HandleClient(sub, fd, events);
          } catch (...) {
            CloseClient(sub, fd);
          }
        })) {
      CloseClient(sub, fd);
    }
  }
  void AcceptReady(std::uint32_t) {
    if (!listener_registered) {
      loop.RegisterFd(listener, EPOLLIN | EPOLLET, [this](std::uint32_t events) { AcceptReady(events); });
      listener_registered = true;
    }
    for (;;) {
      const int fd = accept(listener, nullptr, nullptr);
      if (fd < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) return; return; }
      if (!NonBlocking(fd)) { close(fd); continue; }
      auto* sub = subs[next_sub++ % subs.size()].get();
      if (!sub->loop.QueueInLoop([this, sub, fd] { AddClient(sub, fd); })) close(fd);
    }
  }
#endif
};

TcpServer::TcpServer(std::uint16_t port, std::size_t worker_count, RequestHandler handler)
    : impl_(std::make_unique<Impl>(port, worker_count, std::move(handler))) {}
TcpServer::TcpServer(std::string bind_address, std::uint16_t port, std::size_t worker_count, RequestHandler handler)
    : impl_(std::make_unique<Impl>(std::move(bind_address), port, worker_count, std::move(handler))) {}
TcpServer::~TcpServer() { Stop(); }

bool TcpServer::Start() {
#ifdef _WIN32
  if (!impl_->handler || impl_->started.exchange(true)) return false;
  WSADATA data{}; if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return false;
  impl_->listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); u_long one = 1;
  if (impl_->listener == INVALID_SOCKET || ioctlsocket(impl_->listener, FIONBIO, &one) != 0) { Stop(); return false; }
  sockaddr_in address{}; address.sin_family = AF_INET; address.sin_port = htons(impl_->requested_port);
  if (inet_pton(AF_INET, impl_->requested_address.c_str(), &address.sin_addr) != 1) { Stop(); return false; }
  if (bind(impl_->listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || listen(impl_->listener, SOMAXCONN) != 0) { Stop(); return false; }
  int length = sizeof(address); getsockname(impl_->listener, reinterpret_cast<sockaddr*>(&address), &length); impl_->bound_port.store(ntohs(address.sin_port));
  impl_->loop.SetIdleCallback([this] { impl_->Poll(); });
  return impl_->loop.Start();
#else
  if (!impl_->handler || impl_->started.exchange(true)) return false;
  impl_->listener = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (impl_->listener < 0) { impl_->started.store(false); return false; }
  int reuse = 1; setsockopt(impl_->listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{}; address.sin_family = AF_INET; address.sin_port = htons(impl_->requested_port);
  if (inet_pton(AF_INET, impl_->requested_address.c_str(), &address.sin_addr) != 1) { close(impl_->listener); impl_->listener = -1; impl_->started.store(false); return false; }
  if (bind(impl_->listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || listen(impl_->listener, 4096) != 0) { close(impl_->listener); impl_->listener = -1; impl_->started.store(false); return false; }
  socklen_t length = sizeof(address); getsockname(impl_->listener, reinterpret_cast<sockaddr*>(&address), &length); impl_->bound_port.store(ntohs(address.sin_port));
  const std::size_t count = impl_->worker_count;
  for (std::size_t index = 0; index < count; ++index) impl_->subs.push_back(std::make_unique<Impl::SubReactor>(1024));
  for (auto& sub : impl_->subs) if (!sub->loop.Start()) { Stop(); return false; }
  if (!impl_->loop.Start()) { Stop(); return false; }
  return impl_->loop.QueueInLoop([this] { impl_->AcceptReady(0); });
#endif
}

void TcpServer::Stop() {
#ifdef _WIN32
  if (!impl_ || !impl_->started.exchange(false)) return;
  impl_->workers.Shutdown();
  impl_->loop.Stop(); for (const auto& item : impl_->clients) closesocket(item.first); impl_->clients.clear();
  if (impl_->listener != INVALID_SOCKET) closesocket(impl_->listener); impl_->listener = INVALID_SOCKET; WSACleanup();
#else
  if (!impl_ || !impl_->started.exchange(false)) return;
  impl_->loop.Stop();
  for (auto& sub : impl_->subs) sub->loop.Stop();
  impl_->workers.Shutdown();
  for (auto& sub : impl_->subs) for (const auto& client : sub->clients) close(client.first);
  for (auto& sub : impl_->subs) sub->clients.clear();
  if (impl_->listener >= 0) close(impl_->listener);
  impl_->listener = -1;
#endif
}

std::uint16_t TcpServer::port() const { return impl_->bound_port.load(); }
}  // namespace mq::network
