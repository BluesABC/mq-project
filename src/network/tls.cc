#include "mq/network/tls.h"

#include <limits>
#include <utility>

#ifdef MQ_ENABLE_TLS
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

namespace mq::network {
namespace {

#ifdef MQ_ENABLE_TLS
std::string OpenSslError(const char* fallback) {
  const auto code = ERR_get_error();
  if (code == 0) return fallback;
  char message[256]{};
  ERR_error_string_n(code, message, sizeof(message));
  return message;
}

bool LoadKeyPair(SSL_CTX* context, const TlsOptions& options, std::string* error) {
  if (options.certificate_file.empty() || options.private_key_file.empty()) {
    if (error != nullptr) *error = "TLS certificate and private key are required";
    return false;
  }
  if (SSL_CTX_use_certificate_file(context, options.certificate_file.c_str(), SSL_FILETYPE_PEM) !=
          1 ||
      SSL_CTX_use_PrivateKey_file(context, options.private_key_file.c_str(), SSL_FILETYPE_PEM) !=
          1 ||
      SSL_CTX_check_private_key(context) != 1) {
    if (error != nullptr) *error = OpenSslError("invalid TLS certificate or private key");
    return false;
  }
  return true;
}
#endif

}  // namespace

TlsSessionContext::TlsSessionContext(void* context, std::string server_name)
    : context_(context), server_name_(std::move(server_name)) {}

TlsSessionContext::~TlsSessionContext() {
#ifdef MQ_ENABLE_TLS
  if (context_ != nullptr) SSL_CTX_free(static_cast<SSL_CTX*>(context_));
#endif
}

std::unique_ptr<TlsSessionContext> TlsSessionContext::CreateClient(const TlsOptions& options,
                                                                    std::string* error) {
#ifndef MQ_ENABLE_TLS
  if (error != nullptr) *error = "TLS support was not compiled in";
  return nullptr;
#else
  OPENSSL_init_ssl(0, nullptr);
  SSL_CTX* context = SSL_CTX_new(TLS_client_method());
  if (context == nullptr) {
    if (error != nullptr) *error = OpenSslError("cannot create TLS client context");
    return nullptr;
  }
  SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION);
  if (options.verify_peer) {
    SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);
    const bool loaded = options.ca_file.empty()
                            ? SSL_CTX_set_default_verify_paths(context) == 1
                            : SSL_CTX_load_verify_locations(context, options.ca_file.c_str(), nullptr) ==
                                  1;
    if (!loaded) {
      if (error != nullptr) *error = OpenSslError("cannot load TLS CA file");
      SSL_CTX_free(context);
      return nullptr;
    }
  } else {
    SSL_CTX_set_verify(context, SSL_VERIFY_NONE, nullptr);
  }
  if (!options.certificate_file.empty() || !options.private_key_file.empty()) {
    if (!LoadKeyPair(context, options, error)) {
      SSL_CTX_free(context);
      return nullptr;
    }
  }
  return std::unique_ptr<TlsSessionContext>(
      new TlsSessionContext(context, options.server_name));
#endif
}

std::unique_ptr<TlsSessionContext> TlsSessionContext::CreateServer(const TlsOptions& options,
                                                                    std::string* error) {
#ifndef MQ_ENABLE_TLS
  if (error != nullptr) *error = "TLS support was not compiled in";
  return nullptr;
#else
  OPENSSL_init_ssl(0, nullptr);
  SSL_CTX* context = SSL_CTX_new(TLS_server_method());
  if (context == nullptr) {
    if (error != nullptr) *error = OpenSslError("cannot create TLS server context");
    return nullptr;
  }
  SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION);
  if (!LoadKeyPair(context, options, error)) {
    SSL_CTX_free(context);
    return nullptr;
  }
  if (options.require_client_certificate) {
    if (options.ca_file.empty() ||
        SSL_CTX_load_verify_locations(context, options.ca_file.c_str(), nullptr) != 1) {
      if (error != nullptr) *error = OpenSslError("client CA file is required for mTLS");
      SSL_CTX_free(context);
      return nullptr;
    }
    SSL_CTX_set_verify(context, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
  }
  return std::unique_ptr<TlsSessionContext>(new TlsSessionContext(context, {}));
#endif
}

TlsHandle TlsSessionContext::NewSession(int socket, bool server, std::string* error) {
#ifndef MQ_ENABLE_TLS
  (void)socket;
  (void)server;
  if (error != nullptr) *error = "TLS support was not compiled in";
  return nullptr;
#else
  SSL* session = SSL_new(static_cast<SSL_CTX*>(context_));
  if (session == nullptr || SSL_set_fd(session, socket) != 1) {
    if (error != nullptr) *error = OpenSslError("cannot create TLS session");
    if (session != nullptr) SSL_free(session);
    return nullptr;
  }
  if (server)
    SSL_set_accept_state(session);
  else {
    SSL_set_connect_state(session);
    if (!server_name_.empty()) {
      SSL_set_tlsext_host_name(session, server_name_.c_str());
      if (SSL_set1_host(session, server_name_.c_str()) != 1) {
        if (error != nullptr) *error = "invalid TLS server name";
        SSL_free(session);
        return nullptr;
      }
    }
  }
  return session;
#endif
}

TlsIoResult TlsSessionContext::Handshake(TlsHandle session, std::string* error) {
#ifndef MQ_ENABLE_TLS
  (void)session;
  if (error != nullptr) *error = "TLS support was not compiled in";
  return TlsIoResult::kError;
#else
  const int result = SSL_do_handshake(static_cast<SSL*>(session));
  if (result == 1) return TlsIoResult::kOk;
  const int code = SSL_get_error(static_cast<SSL*>(session), result);
  if (code == SSL_ERROR_WANT_READ) return TlsIoResult::kWantRead;
  if (code == SSL_ERROR_WANT_WRITE) return TlsIoResult::kWantWrite;
  if (error != nullptr) *error = OpenSslError("TLS handshake failed");
  return TlsIoResult::kError;
#endif
}

TlsIoResult TlsSessionContext::Read(TlsHandle session, void* buffer, std::size_t capacity,
                                    std::size_t* size, std::string* error) {
#ifndef MQ_ENABLE_TLS
  (void)session;
  (void)buffer;
  (void)capacity;
  (void)size;
  if (error != nullptr) *error = "TLS support was not compiled in";
  return TlsIoResult::kError;
#else
  if (size == nullptr || capacity == 0 || capacity > std::numeric_limits<int>::max()) {
    if (error != nullptr) *error = "invalid TLS read buffer";
    return TlsIoResult::kError;
  }
  const int result = SSL_read(static_cast<SSL*>(session), buffer, static_cast<int>(capacity));
  if (result > 0) {
    *size = static_cast<std::size_t>(result);
    return TlsIoResult::kOk;
  }
  const int code = SSL_get_error(static_cast<SSL*>(session), result);
  if (code == SSL_ERROR_WANT_READ) return TlsIoResult::kWantRead;
  if (code == SSL_ERROR_WANT_WRITE) return TlsIoResult::kWantWrite;
  if (code == SSL_ERROR_ZERO_RETURN) return TlsIoResult::kClosed;
  if (error != nullptr) *error = OpenSslError("TLS read failed");
  return TlsIoResult::kError;
#endif
}

TlsIoResult TlsSessionContext::Write(TlsHandle session, const void* buffer, std::size_t size,
                                     std::size_t* written, std::string* error) {
#ifndef MQ_ENABLE_TLS
  (void)session;
  (void)buffer;
  (void)size;
  (void)written;
  if (error != nullptr) *error = "TLS support was not compiled in";
  return TlsIoResult::kError;
#else
  if (written == nullptr || size == 0 || size > std::numeric_limits<int>::max()) {
    if (error != nullptr) *error = "invalid TLS write buffer";
    return TlsIoResult::kError;
  }
  const int result = SSL_write(static_cast<SSL*>(session), buffer, static_cast<int>(size));
  if (result > 0) {
    *written = static_cast<std::size_t>(result);
    return TlsIoResult::kOk;
  }
  const int code = SSL_get_error(static_cast<SSL*>(session), result);
  if (code == SSL_ERROR_WANT_READ) return TlsIoResult::kWantRead;
  if (code == SSL_ERROR_WANT_WRITE) return TlsIoResult::kWantWrite;
  if (code == SSL_ERROR_ZERO_RETURN) return TlsIoResult::kClosed;
  if (error != nullptr) *error = OpenSslError("TLS write failed");
  return TlsIoResult::kError;
#endif
}

void TlsSessionContext::CloseSession(TlsHandle session) {
#ifdef MQ_ENABLE_TLS
  if (session != nullptr) {
    SSL_shutdown(static_cast<SSL*>(session));
    SSL_free(static_cast<SSL*>(session));
  }
#else
  (void)session;
#endif
}

}  // namespace mq::network
