#include "mq/core/logger.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

void FiltersAndRotates() {
  const auto root = std::filesystem::temp_directory_path() / "mq_project_logger_test";
  const auto path = root / "broker.log";
  std::error_code error;
  std::filesystem::remove_all(root, error);
  auto& logger = mq::core::Logger::Instance();
  logger.SetLevel(mq::core::LogLevel::kInfo);
  assert(logger.SetFile(path, 80));
  logger.Log(mq::core::LogLevel::kDebug, "hidden");
  logger.Log(mq::core::LogLevel::kInfo, "first record");
  logger.Log(mq::core::LogLevel::kWarn, "second record that triggers file rotation");
  logger.CloseFile();
  assert(ReadFile(path).find("second record") != std::string::npos);
  assert(ReadFile(path.string() + ".1").find("first record") != std::string::npos);
  assert(ReadFile(path.string() + ".1").find("hidden") == std::string::npos);
  std::filesystem::remove_all(root, error);
}

}  // namespace

int main() {
  FiltersAndRotates();
  return 0;
}
