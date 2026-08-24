#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "mq/core/logger.h"
#include "mq/network/tcp_server.h"
#include "mq/server/broker.h"

namespace {
std::atomic<bool> g_stop{false};
void OnSignal(int) { g_stop.store(true, std::memory_order_release); }
struct Config {
  std::string bind_address = "127.0.0.1";
  std::uint16_t bind_port = 9092;
  std::filesystem::path data_dir = "data";
  std::size_t sub_reactor_threads = 0;
  std::uint64_t segment_size = 64ULL * 1024 * 1024;
  std::uint64_t retention_hours = 168;
  std::uint64_t produce_rate_limit = 0;
  std::filesystem::path log_file;
  std::string node_id = "node-local";
  bool replica_follower = false;
  std::vector<mq::server::ReplicationPeer> replica_peers;
};
std::string Trim(std::string value) { const auto first = value.find_first_not_of(" \t\r\n"); if (first == std::string::npos) return {}; const auto last = value.find_last_not_of(" \t\r\n"); return value.substr(first, last - first + 1); }
bool Number(const std::string& text, std::uint64_t* value) { try { std::size_t used = 0; *value = std::stoull(text, &used, 0); return used == text.size(); } catch (...) { return false; } }
bool ParseSize(std::string text, std::uint64_t* value) {
  text = Trim(text); std::uint64_t multiplier = 1;
  if (text.size() > 2 && (text.substr(text.size() - 2) == "Mi" || text.substr(text.size() - 2) == "mi")) { multiplier = 1024ULL * 1024; text.resize(text.size() - 2); }
  else if (text.size() > 1 && (text.back() == 'M' || text.back() == 'm')) { multiplier = 1024ULL * 1024; text.pop_back(); }
  else if (text.size() > 1 && (text.back() == 'G' || text.back() == 'g')) { multiplier = 1024ULL * 1024 * 1024; text.pop_back(); }
  std::uint64_t number = 0; return Number(Trim(text), &number) && number <= UINT64_MAX / multiplier && (*value = number * multiplier, true);
}
bool LoadConfig(const std::filesystem::path& path, Config* config, std::string* error) {
  std::ifstream input(path); if (!input) { if (error) *error = "cannot open config: " + path.string(); return false; }
  std::string line; std::size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number; line = Trim(line); if (line.empty() || line[0] == '#' || line[0] == ';') continue;
    const auto equal = line.find('='); if (equal == std::string::npos) { if (error) *error = "invalid config line " + std::to_string(line_number); return false; }
    const std::string key = Trim(line.substr(0, equal)); const std::string value = Trim(line.substr(equal + 1)); std::uint64_t number = 0;
    if (key == "bind_address") config->bind_address = value;
    else if (key == "bind_port" && Number(value, &number) && number <= 65535) config->bind_port = static_cast<std::uint16_t>(number);
    else if (key == "data_dir") config->data_dir = value;
    else if (key == "sub_reactor_threads" && Number(value, &number)) config->sub_reactor_threads = static_cast<std::size_t>(number);
    else if (key == "segment_size" && ParseSize(value, &config->segment_size)) {}
    else if (key == "retention_hours" && Number(value, &config->retention_hours)) {}
    else if (key == "produce_rate_limit" && Number(value, &config->produce_rate_limit)) {}
    else if (key == "log_file") config->log_file = value;
    else if (key == "node_id") config->node_id = value;
    else if (key == "replica_role") config->replica_follower = value == "follower";
    else if (key == "replica_peers") {
      std::size_t begin = 0;
      while (begin < value.size()) {
        const auto end = value.find(';', begin); const auto item = value.substr(begin, end == std::string::npos ? std::string::npos : end - begin);
        const auto first = item.find(':'); const auto second = first == std::string::npos ? std::string::npos : item.find(':', first + 1);
        if (first == std::string::npos || second == std::string::npos) { if (error) *error = "invalid replica_peers"; return false; }
        std::uint64_t port = 0; if (!Number(item.substr(second + 1), &port) || port > 65535) { if (error) *error = "invalid replica port"; return false; }
        config->replica_peers.push_back({item.substr(0, first), item.substr(first + 1, second - first - 1), static_cast<std::uint16_t>(port), config->replica_follower});
        begin = end == std::string::npos ? value.size() : end + 1;
      }
    }
    else { if (error) *error = "invalid config value for " + key; return false; }
  }
  return true;
}
}

int main(int argc, char** argv) {
  try {
  std::filesystem::path config_path = "conf/broker.conf";
  if (argc == 3 && std::string(argv[1]) == "--config") config_path = argv[2];
  else if (argc != 1) { std::cerr << "usage: mq_broker [--config path]\n"; return 2; }
  Config config; std::string error;
  if (!LoadConfig(config_path, &config, &error)) { std::cerr << error << '\n'; return 1; }
  auto& logger = mq::core::Logger::Instance();
  if (!config.log_file.empty() && !logger.SetFile(config.log_file, 64ULL * 1024 * 1024, &error)) { std::cerr << error << '\n'; return 1; }
  std::signal(SIGINT, OnSignal); std::signal(SIGTERM, OnSignal);
  mq::core::StorageConfig storage_config; storage_config.segment_size_bytes = config.segment_size; storage_config.retention_ms = config.retention_hours * 60ULL * 60 * 1000;
  mq::server::Broker broker(config.data_dir, storage_config);
  if (!broker.Open(&error)) { logger.Log(mq::core::LogLevel::kCritical, error); return 1; }
  broker.ConfigureReplication(config.node_id, config.replica_peers, 0, config.replica_follower);
  broker.ConfigureRateLimit(config.produce_rate_limit);
  broker.StartReplication();
  mq::network::TcpServer server(config.bind_address, config.bind_port, config.sub_reactor_threads, [&broker](const mq::protocol::Request& request) { return broker.Handle(request); });
  if (!server.Start()) { logger.Log(mq::core::LogLevel::kCritical, "cannot start broker server"); return 1; }
  logger.Log(mq::core::LogLevel::kInfo, "broker listening on " + config.bind_address + ":" + std::to_string(server.port()));
  while (!g_stop.load(std::memory_order_acquire)) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  server.Stop();
  broker.StopReplication();
  if (!broker.Flush(&error)) { logger.Log(mq::core::LogLevel::kError, error); return 1; }
  logger.Log(mq::core::LogLevel::kInfo, "broker stopped"); logger.CloseFile(); return 0;
  } catch (const std::exception& exception) {
    std::cerr << "broker startup/runtime exception: " << exception.what() << '\n';
    return 1;
  }
}
