// MQ Broker 服务入口
//
// main.cc 是消息队列 Broker 服务的主程序入口，负责：
// 1. 配置解析：从配置文件读取所有参数
// 2. 服务装配：初始化日志、存储引擎、Broker、网络服务器
// 3. 启动服务：启动复制线程、开始监听客户端连接
// 4. 优雅停机：处理信号、停止服务、刷新数据、关闭日志
//
// 支持的配置项：
// - 网络：bind_address, bind_port, sub_reactor_threads
// - 存储：data_dir, segment_size, retention_hours
// - 复制：node_id, replica_role, replica_peers, replication_auth_token
// - 认证：client_auth_token, client_auth_permissions, client_auth_produce_topics, client_auth_consume_topics
// - TLS：tls_enabled, tls_certificate_file, tls_private_key_file, tls_ca_file, tls_require_client_certificate
// - 限流：produce_rate_limit, topic_produce_quota_bytes
// - 日志：log_file

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <utility>

#include "mq/core/logger.h"
#include "mq/network/tcp_server.h"
#include "mq/server/broker.h"

namespace {

// 全局停止标志，用于信号处理
std::atomic<bool> g_stop{false};

// 信号处理函数，设置停止标志
void OnSignal(int) {
  g_stop.store(true, std::memory_order_release);
}

// 配置结构体，包含所有配置项
struct Config {
  std::string bind_address = "127.0.0.1";  // 监听地址
  std::uint16_t bind_port = 9092;           // 监听端口
  std::filesystem::path data_dir = "data";  // 数据目录
  std::size_t sub_reactor_threads = 0;      // Sub Reactor 线程数（0=自动）
  std::uint64_t segment_size = 64ULL * 1024 * 1024;  // WAL Segment 大小
  std::uint64_t retention_hours = 168;      // 消息保留时间（小时）
  std::uint64_t produce_rate_limit = 0;     // 生产请求限流（0=不限）
  std::uint64_t topic_produce_quota_bytes = 0;  // Topic 字节配额（0=不限）
  std::filesystem::path log_file;           // 日志文件路径
  std::string node_id = "node-local";       // 节点 ID
  bool replica_follower = false;            // 是否为 Follower 角色
  std::string replication_auth_token;       // 复制认证 Token
  std::string client_auth_token;            // 客户端认证 Token
  std::string client_auth_permissions;      // 客户端权限（admin/produce/consume）
  std::string client_auth_produce_topics;   // 允许生产的 Topic 列表
  std::string client_auth_consume_topics;   // 允许消费的 Topic 列表
  bool tls_enabled = false;                 // 是否启用 TLS
  std::filesystem::path tls_certificate_file;  // TLS 证书文件
  std::filesystem::path tls_private_key_file;  // TLS 私钥文件
  std::filesystem::path tls_ca_file;            // TLS CA 证书文件
  bool tls_require_client_certificate = false;  // 是否要求客户端证书
  std::vector<mq::server::ReplicationPeer> replica_peers;  // 副本节点列表
};

// 去除字符串两端的空白字符
std::string Trim(std::string value) {
  const auto first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return {};
  const auto last = value.find_last_not_of(" \t\r\n");
  return value.substr(first, last - first + 1);
}

// 解析逗号分隔的列表
bool ParseList(const std::string& text, std::vector<std::string>* values) {
  if (values == nullptr) return false;
  values->clear();
  std::size_t begin = 0;
  while (begin <= text.size()) {
    const auto end = text.find(',', begin);
    const auto item = Trim(text.substr(begin, end == std::string::npos ? std::string::npos
                                                                        : end - begin));
    if (item.empty()) return false;
    values->push_back(item);
    if (end == std::string::npos) break;
    begin = end + 1;
  }
  return true;
}

// 构建客户端授权配置
// 解析权限字符串和 Topic 白名单
bool BuildClientAuthorization(const Config& config, mq::server::ClientAuthorization* authorization,
                              std::string* error) {
  if (authorization == nullptr) return false;
  *authorization = {};
  // 解析权限
  if (!config.client_auth_permissions.empty()) {
    authorization->allow_admin = false;
    authorization->allow_produce = false;
    authorization->allow_consume = false;
    std::vector<std::string> permissions;
    if (!ParseList(config.client_auth_permissions, &permissions)) {
      if (error) *error = "invalid client_auth_permissions";
      return false;
    }
    for (const auto& permission : permissions) {
      if (permission == "admin")
        authorization->allow_admin = true;
      else if (permission == "produce")
        authorization->allow_produce = true;
      else if (permission == "consume")
        authorization->allow_consume = true;
      else {
        if (error) *error = "invalid client authorization permission";
        return false;
      }
    }
  }
  // 解析生产 Topic 白名单
  if (!config.client_auth_produce_topics.empty() &&
      !ParseList(config.client_auth_produce_topics, &authorization->produce_topics)) {
    if (error) *error = "invalid client_auth_produce_topics";
    return false;
  }
  // 解析消费 Topic 白名单
  if (!config.client_auth_consume_topics.empty() &&
      !ParseList(config.client_auth_consume_topics, &authorization->consume_topics)) {
    if (error) *error = "invalid client_auth_consume_topics";
    return false;
  }
  return true;
}

// 解析无符号整数
bool Number(const std::string& text, std::uint64_t* value) {
  try {
    std::size_t used = 0;
    *value = std::stoull(text, &used, 0);
    return used == text.size();
  } catch (...) {
    return false;
  }
}

// 解析带单位的大小（M/G）
bool ParseSize(std::string text, std::uint64_t* value) {
  text = Trim(text);
  std::uint64_t multiplier = 1;
  if (text.size() > 2 &&
      (text.substr(text.size() - 2) == "Mi" || text.substr(text.size() - 2) == "mi")) {
    multiplier = 1024ULL * 1024;
    text.resize(text.size() - 2);
  } else if (text.size() > 1 && (text.back() == 'M' || text.back() == 'm')) {
    multiplier = 1024ULL * 1024;
    text.pop_back();
  } else if (text.size() > 1 && (text.back() == 'G' || text.back() == 'g')) {
    multiplier = 1024ULL * 1024 * 1024;
    text.pop_back();
  }
  std::uint64_t number = 0;
  return Number(Trim(text), &number) && number <= UINT64_MAX / multiplier &&
         (*value = number * multiplier, true);
}

// 加载配置文件
// 解析 INI 格式的配置文件
bool LoadConfig(const std::filesystem::path& path, Config* config, std::string* error) {
  std::ifstream input(path);
  if (!input) {
    if (error) *error = "cannot open config: " + path.string();
    return false;
  }
  std::string line;
  std::size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    line = Trim(line);
    // 跳过空行和注释
    if (line.empty() || line[0] == '#' || line[0] == ';') continue;
    const auto equal = line.find('=');
    if (equal == std::string::npos) {
      if (error) *error = "invalid config line " + std::to_string(line_number);
      return false;
    }
    const std::string key = Trim(line.substr(0, equal));
    const std::string value = Trim(line.substr(equal + 1));
    std::uint64_t number = 0;
    // 解析各配置项
    if (key == "bind_address")
      config->bind_address = value;
    else if (key == "bind_port" && Number(value, &number) && number <= 65535)
      config->bind_port = static_cast<std::uint16_t>(number);
    else if (key == "data_dir")
      config->data_dir = value;
    else if (key == "sub_reactor_threads" && Number(value, &number))
      config->sub_reactor_threads = static_cast<std::size_t>(number);
    else if (key == "segment_size" && ParseSize(value, &config->segment_size)) {
    } else if (key == "retention_hours" && Number(value, &config->retention_hours)) {
    } else if (key == "produce_rate_limit" && Number(value, &config->produce_rate_limit)) {
    } else if (key == "topic_produce_quota_bytes" &&
               ParseSize(value, &config->topic_produce_quota_bytes)) {
    } else if (key == "log_file")
      config->log_file = value;
    else if (key == "node_id")
      config->node_id = value;
    else if (key == "replica_role")
      config->replica_follower = value == "follower";
    else if (key == "replication_auth_token")
      config->replication_auth_token = value;
    else if (key == "client_auth_token")
      config->client_auth_token = value;
    else if (key == "client_auth_permissions")
      config->client_auth_permissions = value;
    else if (key == "client_auth_produce_topics")
      config->client_auth_produce_topics = value;
    else if (key == "client_auth_consume_topics")
      config->client_auth_consume_topics = value;
    else if (key == "tls_enabled")
      config->tls_enabled = value == "true" || value == "1";
    else if (key == "tls_certificate_file")
      config->tls_certificate_file = value;
    else if (key == "tls_private_key_file")
      config->tls_private_key_file = value;
    else if (key == "tls_ca_file")
      config->tls_ca_file = value;
    else if (key == "tls_require_client_certificate")
      config->tls_require_client_certificate = value == "true" || value == "1";
    else if (key == "replica_peers") {
      // 解析副本节点列表：node-id:host:port;node-id:host:port;...
      std::size_t begin = 0;
      while (begin < value.size()) {
        const auto end = value.find(';', begin);
        const auto item =
            value.substr(begin, end == std::string::npos ? std::string::npos : end - begin);
        const auto first = item.find(':');
        const auto second =
            first == std::string::npos ? std::string::npos : item.find(':', first + 1);
        if (first == std::string::npos || second == std::string::npos) {
          if (error) *error = "invalid replica_peers";
          return false;
        }
        std::uint64_t port = 0;
        if (!Number(item.substr(second + 1), &port) || port > 65535) {
          if (error) *error = "invalid replica port";
          return false;
        }
        config->replica_peers.push_back(
            {item.substr(0, first), item.substr(first + 1, second - first - 1),
             static_cast<std::uint16_t>(port), config->replica_follower});
        begin = end == std::string::npos ? value.size() : end + 1;
      }
    } else {
      if (error) *error = "invalid config value for " + key;
      return false;
    }
  }
  // 设置 Peer 的 leader 标志
  for (auto& peer : config->replica_peers) peer.leader = config->replica_follower;
  return true;
}

}  // namespace

// 主函数，Broker 服务入口
int main(int argc, char** argv) {
  try {
    // 解析命令行参数
    std::filesystem::path config_path = "conf/broker.conf";
    if (argc == 3 && std::string(argv[1]) == "--config")
      config_path = argv[2];
    else if (argc != 1) {
      std::cerr << "usage: mq_broker [--config path]\n";
      return 2;
    }
    // 加载配置
    Config config;
    std::string error;
    if (!LoadConfig(config_path, &config, &error)) {
      std::cerr << error << '\n';
      return 1;
    }
    // 配置校验
    if (!config.replica_peers.empty() && config.replication_auth_token.empty()) {
      std::cerr << "replication_auth_token is required when replica_peers is configured\n";
      return 1;
    }
    if (config.client_auth_token.empty() &&
        (!config.client_auth_permissions.empty() ||
         !config.client_auth_produce_topics.empty() ||
         !config.client_auth_consume_topics.empty())) {
      std::cerr << "client_auth_token is required when client ACL is configured\n";
      return 1;
    }
    // 初始化日志
    auto& logger = mq::core::Logger::Instance();
    if (!config.log_file.empty() && !logger.SetFile(config.log_file, 64ULL * 1024 * 1024, &error)) {
      std::cerr << error << '\n';
      return 1;
    }
    // 注册信号处理器
    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);
    // 创建存储配置
    mq::core::StorageConfig storage_config;
    storage_config.segment_size_bytes = config.segment_size;
    storage_config.retention_ms = config.retention_hours * 60ULL * 60 * 1000;
    // 创建并打开 Broker
    mq::server::Broker broker(config.data_dir, storage_config);
    if (!broker.Open(&error)) {
      logger.Log(mq::core::LogLevel::kCritical, error);
      return 1;
    }
    // 配置复制
    broker.ConfigureReplication(config.node_id, config.replica_peers, 0, config.replica_follower,
                                config.replication_auth_token);
    // 配置客户端认证
    mq::server::ClientAuthorization authorization;
    if (!BuildClientAuthorization(config, &authorization, &error)) {
      std::cerr << error << '\n';
      return 1;
    }
    broker.ConfigureClientAuth(config.client_auth_token, std::move(authorization));
    // 配置 TLS
    mq::network::TlsOptions tls_options;
    tls_options.enabled = config.tls_enabled;
    tls_options.certificate_file = config.tls_certificate_file.string();
    tls_options.private_key_file = config.tls_private_key_file.string();
    tls_options.ca_file = config.tls_ca_file.string();
    tls_options.require_client_certificate = config.tls_require_client_certificate;
    // 配置限流和配额
    broker.ConfigureRateLimit(config.produce_rate_limit);
    broker.ConfigureTopicQuota(config.topic_produce_quota_bytes);
    // 启动复制线程
    broker.StartReplication();
    // 创建并启动 TCP 服务器
    mq::network::TcpServer server(
        config.bind_address, config.bind_port, config.sub_reactor_threads,
        [&broker](const mq::protocol::Request& request) { return broker.Handle(request); },
        std::move(tls_options));
    if (!server.Start()) {
      logger.Log(mq::core::LogLevel::kCritical, "cannot start broker server");
      return 1;
    }
    logger.Log(mq::core::LogLevel::kInfo,
               "broker listening on " + config.bind_address + ":" + std::to_string(server.port()));
    // 主循环：等待停止信号
    while (!g_stop.load(std::memory_order_acquire))
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // 优雅停机
    server.Stop();
    broker.StopReplication();
    if (!broker.Flush(&error)) {
      logger.Log(mq::core::LogLevel::kError, error);
      return 1;
    }
    logger.Log(mq::core::LogLevel::kInfo, "broker stopped");
    logger.CloseFile();
    return 0;
  } catch (const std::exception& exception) {
    std::cerr << "broker startup/runtime exception: " << exception.what() << '\n';
    return 1;
  }
}