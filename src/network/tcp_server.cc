#include "mq/network/tcp_server.h"

#include <algorithm>
#include <atomic>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mq/core/memory_pool.h"
#include "mq/core/thread_pool.h"
#include "mq/network/tcp_connection.h"
#include "mq/network/tls.h"
#include "mq/protocol/protocol_codec.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#endif

namespace mq::network {

struct TcpServer::Impl {
  static constexpr std::size_t kConnectionReadBufferBytes = 256 * 1024;
  static constexpr std::size_t kConnectionWriteBufferBytes = protocol::kMaxPayloadBytes + 18;
  static std::size_t DefaultWorkers(std::size_t count) {
    constexpr std::size_t kMaxAutomaticWorkers = 32;
    const auto cores = std::thread::hardware_concurrency();
    if (count != 0) return count;
    if (cores == 0) return 1;
    return std::min<std::size_t>(cores, kMaxAutomaticWorkers);
  }
  Impl(std::uint16_t configured_port, std::size_t worker_count, RequestHandler configured_handler,
       TlsOptions configured_tls_options)
      : requested_port(configured_port),
        worker_count(DefaultWorkers(worker_count)),
        handler(std::move(configured_handler)),
        tls_options(std::move(configured_tls_options)),
        workers(DefaultWorkers(worker_count), 1024),
        loop(1024) {}
  Impl(std::string address, std::uint16_t configured_port, std::size_t count,
       RequestHandler configured_handler, TlsOptions configured_tls_options)
      : requested_address(std::move(address)),
        requested_port(configured_port),
        worker_count(DefaultWorkers(count)),
        handler(std::move(configured_handler)),
        tls_options(std::move(configured_tls_options)),
        workers(DefaultWorkers(count), 1024),
        loop(1024) {}
  std::string requested_address = "127.0.0.1";
  std::uint16_t requested_port;
  std::size_t worker_count;
  std::atomic<std::uint16_t> bound_port{0};
  RequestHandler handler;
  TlsOptions tls_options;
  std::unique_ptr<TlsSessionContext> tls_context;
  core::ThreadPool workers;
  EventLoop loop;
  std::atomic<bool> started{false};
#ifdef _WIN32
  struct Client {
    std::shared_ptr<core::MemoryPool> pool;
    std::shared_ptr<TcpConnection> connection;
    TlsHandle tls_session = nullptr;
    bool tls_ready = false;
    bool tls_want_write = false;
  };
  SOCKET listener = INVALID_SOCKET;
  std::map<SOCKET, Client> clients;
  std::uint64_t next_id = 1;

  void CloseClient(SOCKET socket) {
    const auto it = clients.find(socket);
    if (it != clients.end() && it->second.tls_session != nullptr)
      tls_context->CloseSession(it->second.tls_session);
    closesocket(socket);
    clients.erase(socket);
  }
  void Poll() {
    fd_set reads, writes;
    FD_ZERO(&reads);
    FD_ZERO(&writes);
    FD_SET(listener, &reads);
    for (const auto& item : clients) {
      FD_SET(item.first, &reads);
      if (item.second.tls_want_write || !item.second.connection->Writable().empty())
        FD_SET(item.first, &writes);
    }
    timeval timeout{};
    if (select(0, &reads, &writes, nullptr, &timeout) <= 0) return;
    if (FD_ISSET(listener, &reads))
      for (;;) {
        SOCKET socket = accept(listener, nullptr, nullptr);
        if (socket == INVALID_SOCKET) break;
        u_long one = 1;
        if (ioctlsocket(socket, FIONBIO, &one) != 0) {
          closesocket(socket);
          continue;
        }
        Client client;
        client.pool = std::make_shared<core::MemoryPool>(512 * 1024);
        client.connection = std::make_shared<TcpConnection>(
            next_id++, &loop, client.pool, kConnectionReadBufferBytes, kConnectionWriteBufferBytes);
        if (tls_options.enabled) {
          std::string error;
          client.tls_session = tls_context->NewSession(static_cast<int>(socket), true, &error);
          if (client.tls_session == nullptr) {
            closesocket(socket);
            continue;
          }
        } else {
          client.tls_ready = true;
        }
        clients.emplace(socket, std::move(client));
      }
    char input[8192];
    std::vector<SOCKET> closed;
    for (auto& item : clients) {
      if (!item.second.tls_ready &&
          (FD_ISSET(item.first, &reads) || FD_ISSET(item.first, &writes))) {
        std::string error;
        const auto result = tls_context->Handshake(item.second.tls_session, &error);
        if (result == TlsIoResult::kOk) {
          item.second.tls_ready = true;
          item.second.tls_want_write = false;
        } else if (result == TlsIoResult::kWantWrite) {
          item.second.tls_want_write = true;
        } else if (result == TlsIoResult::kWantRead) {
          item.second.tls_want_write = false;
        } else {
          closed.push_back(item.first);
          continue;
        }
        if (!item.second.tls_ready) continue;
      }
      if (FD_ISSET(item.first, &reads)) {
        int count = 0;
        if (item.second.tls_session != nullptr) {
          std::size_t read = 0;
          std::string error;
          const auto result =
              tls_context->Read(item.second.tls_session, input, sizeof(input), &read, &error);
          if (result == TlsIoResult::kWantWrite) {
            item.second.tls_want_write = true;
            continue;
          }
          if (result == TlsIoResult::kWantRead) continue;
          if (result != TlsIoResult::kOk) {
            closed.push_back(item.first);
            continue;
          }
          count = static_cast<int>(read);
        } else {
          count = recv(item.first, input, sizeof(input), 0);
        }
        if (count <= 0) {
          closed.push_back(item.first);
          continue;
        }
        std::vector<protocol::Request> requests;
        std::string error;
        if (!item.second.connection->DecodeRequests(std::string_view(input, count), &requests,
                                                    &error)) {
          closed.push_back(item.first);
          continue;
        }
        for (const auto& request : requests) {
          const auto connection = item.second.connection;
          if (!workers.Submit([this, connection, request] {
                protocol::Response response = handler(request);
                std::string frame;
                if (protocol::ProtocolCodec::EncodeResponse(response, &frame))
                  connection->Send(std::move(frame));
              }))
            {
              protocol::Response response;
              response.status = protocol::Status::kResourceExhausted;
              response.request_id = request.request_id;
              std::string frame;
              if (!protocol::ProtocolCodec::EncodeResponse(response, &frame) ||
                  !item.second.connection->Send(std::move(frame)))
                closed.push_back(item.first);
            }
        }
      }
      if (FD_ISSET(item.first, &writes)) {
        const std::string_view output = item.second.connection->Writable();
        if (output.empty()) {
          item.second.tls_want_write = false;
          continue;
        }
        int count = 0;
        if (item.second.tls_session != nullptr) {
          std::size_t written = 0;
          std::string error;
          const auto result =
              tls_context->Write(item.second.tls_session, output.data(), output.size(), &written,
                                  &error);
          if (result == TlsIoResult::kWantRead) {
            item.second.tls_want_write = false;
            continue;
          }
          if (result == TlsIoResult::kWantWrite) {
            item.second.tls_want_write = true;
            continue;
          }
          if (result != TlsIoResult::kOk) {
            closed.push_back(item.first);
            continue;
          }
          count = static_cast<int>(written);
        } else {
          count = send(item.first, output.data(), static_cast<int>(output.size()), 0);
        }
        if (count > 0) {
          item.second.connection->ConsumeWritten(static_cast<std::size_t>(count));
          item.second.tls_want_write = false;
        } else {
          closed.push_back(item.first);
        }
      }
    }
    for (SOCKET socket : closed)
      if (clients.find(socket) != clients.end()) CloseClient(socket);
  }
#else
  struct Client {
    std::shared_ptr<core::MemoryPool> pool;
    std::shared_ptr<TcpConnection> connection;
    TlsHandle tls_session = nullptr;
    bool tls_ready = false;
    bool tls_want_write = false;
  };
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
    if (it->second.tls_session != nullptr) tls_context->CloseSession(it->second.tls_session);
    close(fd);
    sub->clients.erase(fd);
  }
  void HandleClient(SubReactor* sub, int fd, std::uint32_t events) {
    auto it = sub->clients.find(fd);
    if (it == sub->clients.end()) return;
    if ((events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
      CloseClient(sub, fd);
      return;
    }
    if (!it->second.tls_ready) {
      std::string error;
      const auto result = tls_context->Handshake(it->second.tls_session, &error);
      if (result == TlsIoResult::kOk) {
        it->second.tls_ready = true;
        it->second.tls_want_write = false;
      } else if (result == TlsIoResult::kWantWrite) {
        it->second.tls_want_write = true;
      } else if (result == TlsIoResult::kWantRead) {
        it->second.tls_want_write = false;
      } else {
        CloseClient(sub, fd);
        return;
      }
      if (!it->second.tls_ready) {
        sub->loop.ModifyFd(fd, EPOLLIN | EPOLLET | EPOLLRDHUP |
                                   (it->second.tls_want_write ? EPOLLOUT : 0));
        return;
      }
    }
    if ((events & EPOLLIN) != 0) {
      char buffer[64 * 1024];
      for (;;) {
        ssize_t count = 0;
        if (it->second.tls_session != nullptr) {
          std::size_t read = 0;
          std::string error;
          const auto result =
              tls_context->Read(it->second.tls_session, buffer, sizeof(buffer), &read, &error);
          if (result == TlsIoResult::kWantWrite) {
            it->second.tls_want_write = true;
            break;
          }
          if (result == TlsIoResult::kWantRead) break;
          if (result != TlsIoResult::kOk) {
            CloseClient(sub, fd);
            return;
          }
          count = static_cast<ssize_t>(read);
        } else {
          count = recv(fd, buffer, sizeof(buffer), 0);
        }
        if (count > 0) {
          std::vector<protocol::Request> requests;
          std::string error;
          if (!it->second.connection->DecodeRequests(
                  std::string_view(buffer, static_cast<std::size_t>(count)), &requests, &error)) {
            CloseClient(sub, fd);
            return;
          }
          // 解码留在连接线程，业务处理投递到 Worker；这样网络线程不会执行存储或业务计算。
          for (const auto& request : requests) {
            const auto connection = it->second.connection;
            if (!workers.Submit([this, connection, request, sub, fd] {
                  protocol::Response response = handler(request);
                  std::string frame;
                  if (protocol::ProtocolCodec::EncodeResponse(response, &frame) &&
                      connection->Send(std::move(frame))) {
                    sub->loop.QueueInLoop([sub, fd, connection] {
                      const auto current = sub->clients.find(fd);
                      if (current == sub->clients.end() || current->second.connection != connection)
                        return;
                      sub->loop.ModifyFd(fd, EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP);
                    });
                  }
                })) {
              protocol::Response response;
              response.status = protocol::Status::kResourceExhausted;
              response.request_id = request.request_id;
              std::string frame;
              if (!protocol::ProtocolCodec::EncodeResponse(response, &frame) ||
                  !connection->Send(std::move(frame))) {
                CloseClient(sub, fd);
                return;
              }
            }
          }
          continue;
        }
        if (count == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
          CloseClient(sub, fd);
          return;
        }
        break;
      }
    }
    if ((events & EPOLLOUT) != 0) {
      while (!it->second.connection->Writable().empty()) {
        const auto output = it->second.connection->Writable();
        ssize_t count = 0;
        if (it->second.tls_session != nullptr) {
          std::size_t written = 0;
          std::string error;
          const auto result =
              tls_context->Write(it->second.tls_session, output.data(), output.size(), &written,
                                 &error);
          if (result == TlsIoResult::kWantRead) {
            it->second.tls_want_write = false;
            break;
          }
          if (result == TlsIoResult::kWantWrite) {
            it->second.tls_want_write = true;
            break;
          }
          if (result != TlsIoResult::kOk) {
            CloseClient(sub, fd);
            return;
          }
          count = static_cast<ssize_t>(written);
        } else {
          count = send(fd, output.data(), output.size(), MSG_NOSIGNAL);
        }
        if (count > 0) {
          it->second.connection->ConsumeWritten(static_cast<std::size_t>(count));
          it->second.tls_want_write = false;
          continue;
        }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        CloseClient(sub, fd);
        return;
      }
    }
    sub->loop.ModifyFd(fd, EPOLLIN | EPOLLET | EPOLLRDHUP |
                               (it->second.tls_want_write ||
                                        !it->second.connection->Writable().empty()
                                    ? EPOLLOUT
                                    : 0));
  }
  void AddClient(SubReactor* sub, int fd) {
    auto pool = std::make_shared<core::MemoryPool>(512 * 1024);
    Client client{pool, std::make_shared<TcpConnection>(next_id++, &sub->loop, pool,
                                                        kConnectionReadBufferBytes,
                                                        kConnectionWriteBufferBytes)};
    if (tls_options.enabled) {
      std::string error;
      client.tls_session = tls_context->NewSession(fd, true, &error);
      if (client.tls_session == nullptr) {
        close(fd);
        return;
      }
    } else {
      client.tls_ready = true;
    }
    const auto inserted = sub->clients.emplace(fd, std::move(client));
    if (!inserted.second) {
      close(fd);
      return;
    }
    if (!sub->loop.RegisterFd(fd, EPOLLIN | EPOLLET | EPOLLRDHUP,
                              [this, sub, fd](std::uint32_t events) {
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
      loop.RegisterFd(listener, EPOLLIN | EPOLLET,
                      [this](std::uint32_t events) { AcceptReady(events); });
      listener_registered = true;
    }
    for (;;) {
      const int fd = accept(listener, nullptr, nullptr);
      if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        return;
      }
      if (!NonBlocking(fd)) {
        close(fd);
        continue;
      }
      // 轮询分发连接，连接建立后始终绑定到该 Sub Reactor。
      auto* sub = subs[next_sub++ % subs.size()].get();
      if (!sub->loop.QueueInLoop([this, sub, fd] { AddClient(sub, fd); })) close(fd);
    }
  }
#endif
};

TcpServer::TcpServer(std::uint16_t port, std::size_t worker_count, RequestHandler handler,
                     TlsOptions tls_options)
    : impl_(std::make_unique<Impl>(port, worker_count, std::move(handler),
                                   std::move(tls_options))) {}
TcpServer::TcpServer(std::string bind_address, std::uint16_t port, std::size_t worker_count,
                     RequestHandler handler, TlsOptions tls_options)
    : impl_(std::make_unique<Impl>(std::move(bind_address), port, worker_count,
                                   std::move(handler), std::move(tls_options))) {}
TcpServer::~TcpServer() {
  Stop();
}

bool TcpServer::Start() {
#ifdef _WIN32
  if (!impl_->handler || impl_->started.exchange(true)) return false;
  if (impl_->tls_options.enabled) {
    std::string error;
    impl_->tls_context = TlsSessionContext::CreateServer(impl_->tls_options, &error);
    if (!impl_->tls_context) {
      impl_->started.store(false);
      return false;
    }
  }
  WSADATA data{};
  if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return false;
  impl_->listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  u_long one = 1;
  if (impl_->listener == INVALID_SOCKET || ioctlsocket(impl_->listener, FIONBIO, &one) != 0) {
    Stop();
    return false;
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(impl_->requested_port);
  if (inet_pton(AF_INET, impl_->requested_address.c_str(), &address.sin_addr) != 1) {
    Stop();
    return false;
  }
  if (bind(impl_->listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(impl_->listener, SOMAXCONN) != 0) {
    Stop();
    return false;
  }
  int length = sizeof(address);
  getsockname(impl_->listener, reinterpret_cast<sockaddr*>(&address), &length);
  impl_->bound_port.store(ntohs(address.sin_port));
  impl_->loop.SetIdleCallback([this] { impl_->Poll(); });
  return impl_->loop.Start();
#else
  if (!impl_->handler || impl_->started.exchange(true)) return false;
  if (impl_->tls_options.enabled) {
    std::string error;
    impl_->tls_context = TlsSessionContext::CreateServer(impl_->tls_options, &error);
    if (!impl_->tls_context) {
      impl_->started.store(false);
      return false;
    }
  }
  impl_->listener = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (impl_->listener < 0) {
    impl_->started.store(false);
    return false;
  }
  int reuse = 1;
  setsockopt(impl_->listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(impl_->requested_port);
  if (inet_pton(AF_INET, impl_->requested_address.c_str(), &address.sin_addr) != 1) {
    close(impl_->listener);
    impl_->listener = -1;
    impl_->started.store(false);
    return false;
  }
  if (bind(impl_->listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(impl_->listener, 4096) != 0) {
    close(impl_->listener);
    impl_->listener = -1;
    impl_->started.store(false);
    return false;
  }
  socklen_t length = sizeof(address);
  getsockname(impl_->listener, reinterpret_cast<sockaddr*>(&address), &length);
  impl_->bound_port.store(ntohs(address.sin_port));
  const std::size_t count = impl_->worker_count;
  for (std::size_t index = 0; index < count; ++index)
    impl_->subs.push_back(std::make_unique<Impl::SubReactor>(1024));
  for (auto& sub : impl_->subs)
    if (!sub->loop.Start()) {
      Stop();
      return false;
    }
  if (!impl_->loop.Start()) {
    Stop();
    return false;
  }
  return impl_->loop.QueueInLoop([this] { impl_->AcceptReady(0); });
#endif
}

void TcpServer::Stop() {
#ifdef _WIN32
  if (!impl_ || !impl_->started.exchange(false)) return;
  impl_->workers.Shutdown();
  impl_->loop.Stop();
  for (const auto& item : impl_->clients) {
    if (item.second.tls_session != nullptr)
      impl_->tls_context->CloseSession(item.second.tls_session);
    closesocket(item.first);
  }
  impl_->clients.clear();
  if (impl_->listener != INVALID_SOCKET) closesocket(impl_->listener);
  impl_->listener = INVALID_SOCKET;
  WSACleanup();
#else
  if (!impl_ || !impl_->started.exchange(false)) return;
  impl_->loop.Stop();
  for (auto& sub : impl_->subs) sub->loop.Stop();
  impl_->workers.Shutdown();
  for (auto& sub : impl_->subs)
    for (const auto& client : sub->clients) {
      if (client.second.tls_session != nullptr)
        impl_->tls_context->CloseSession(client.second.tls_session);
      close(client.first);
    }
  for (auto& sub : impl_->subs) sub->clients.clear();
  if (impl_->listener >= 0) close(impl_->listener);
  impl_->listener = -1;
#endif
}

std::uint16_t TcpServer::port() const {
  return impl_->bound_port.load();
}
}  // namespace mq::network
