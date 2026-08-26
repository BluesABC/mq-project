#pragma once

#include <cstddef>
#include <memory>
#include <string>

namespace mq::network {

struct TlsOptions {
  bool enabled = false;
  std::string certificate_file;
  std::string private_key_file;
  std::string ca_file;
  std::string server_name;
  bool verify_peer = true;
  bool require_client_certificate = false;
};

enum class TlsIoResult { kOk, kWantRead, kWantWrite, kClosed, kError };
using TlsHandle = void*;

// 封装 OpenSSL 句柄，业务层只处理非阻塞 I/O 状态，不直接依赖 OpenSSL 类型。
class TlsSessionContext {
 public:
  ~TlsSessionContext();
  TlsSessionContext(const TlsSessionContext&) = delete;
  TlsSessionContext& operator=(const TlsSessionContext&) = delete;

  static std::unique_ptr<TlsSessionContext> CreateClient(const TlsOptions& options,
                                                         std::string* error);
  static std::unique_ptr<TlsSessionContext> CreateServer(const TlsOptions& options,
                                                         std::string* error);
  TlsHandle NewSession(int socket, bool server, std::string* error);
  TlsIoResult Handshake(TlsHandle session, std::string* error);
  TlsIoResult Read(TlsHandle session, void* buffer, std::size_t capacity, std::size_t* size,
                   std::string* error);
  TlsIoResult Write(TlsHandle session, const void* buffer, std::size_t size, std::size_t* written,
                    std::string* error);
  void CloseSession(TlsHandle session);

 private:
  explicit TlsSessionContext(void* context, std::string server_name);
  void* context_ = nullptr;
  std::string server_name_;
};

}  // namespace mq::network
