#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <string>

#include "mq/client/mq_client.h"
#include "mq/network/tcp_server.h"

namespace {

std::string Quote(const std::filesystem::path& path) {
  return "\"" + path.string() + "\"";
}

}  // namespace

int main() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_tls_test";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  std::filesystem::create_directories(root);
  const auto certificate = root / "server.crt";
  const auto private_key = root / "server.key";
  const std::string command =
      "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost "
      "-addext subjectAltName=IP:127.0.0.1 -keyout " +
      Quote(private_key) + " -out " + Quote(certificate) + " >/dev/null 2>&1";
  assert(std::system(command.c_str()) == 0);

  mq::network::TlsOptions server_tls;
  server_tls.enabled = true;
  server_tls.certificate_file = certificate.string();
  server_tls.private_key_file = private_key.string();
  mq::network::TcpServer server(
      "127.0.0.1", 0, 1,
      [](const mq::protocol::Request& request) {
        mq::protocol::Response response;
        response.request_id = request.request_id;
        return response;
      },
      server_tls);
  assert(server.Start());

  mq::client::MqProducer client;
  client.setTimeoutMs(3000);
  mq::network::TlsOptions client_tls;
  client_tls.enabled = true;
  client_tls.ca_file = certificate.string();
  client_tls.server_name = "127.0.0.1";
  client.setTlsOptions(client_tls);
  assert(client.connect("127.0.0.1", server.port()));
  assert(client.flush());
  client.close();
  server.Stop();
  std::filesystem::remove_all(root, error);
  return 0;
}
