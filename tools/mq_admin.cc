#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "mq/client/mq_client.h"

namespace {

struct Options {
  std::string command;
  std::string host = "127.0.0.1";
  std::uint16_t port = 9092;
  std::uint32_t timeout_ms = 5000;
};

void PrintUsage() {
  std::cout << "usage: mq_admin metrics|topics [--host HOST] [--port PORT] [--timeout-ms MS]\n";
}

bool ParseNumber(const std::string& text, std::uint64_t* value) {
  try {
    std::size_t used = 0;
    *value = std::stoull(text, &used, 10);
    return used == text.size();
  } catch (...) {
    return false;
  }
}

bool Parse(int argc, char** argv, Options* options) {
  if (argc == 2 && std::string(argv[1]) == "--help") return false;
  if (argc < 2) return false;
  options->command = argv[1];
  for (int index = 2; index < argc; ++index) {
    const std::string name = argv[index];
    if ((name == "--host" || name == "--port" || name == "--timeout-ms") && index + 1 < argc) {
      const std::string value = argv[++index];
      if (name == "--host")
        options->host = value;
      else {
        std::uint64_t number = 0;
        if (!ParseNumber(value, &number) || (name == "--port" && (number == 0 || number > 65535)) ||
            (name == "--timeout-ms" && (number == 0 || number > UINT32_MAX)))
          return false;
        if (name == "--port")
          options->port = static_cast<std::uint16_t>(number);
        else
          options->timeout_ms = static_cast<std::uint32_t>(number);
      }
    } else {
      return false;
    }
  }
  return options->command == "metrics" || options->command == "topics";
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!Parse(argc, argv, &options)) {
    PrintUsage();
    return argc == 2 && std::string(argv[1]) == "--help" ? 0 : 2;
  }

  mq::client::MqProducer client;
  client.setTimeoutMs(options.timeout_ms);
  if (!client.connect(options.host, options.port)) {
    std::cerr << "connect failed: " << client.lastError() << '\n';
    return 1;
  }
  if (options.command == "metrics") {
    std::string output;
    if (!client.metrics(&output)) {
      std::cerr << "metrics failed: " << client.lastError() << '\n';
      return 1;
    }
    std::cout << output;
    return 0;
  }

  std::vector<mq::client::TopicInfo> topics;
  if (!client.listTopics(&topics)) {
    std::cerr << "topics failed: " << client.lastError() << '\n';
    return 1;
  }
  std::cout << "topic\tpartitions\n";
  for (const auto& topic : topics) std::cout << topic.name << '\t' << topic.partitions << '\n';
  return 0;
}
