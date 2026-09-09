/**
 * @file logger_test.cc
 * @brief 日志系统单元测试
 * 
 * 本文件包含对日志系统功能的单元测试，验证以下功能：
 * 1. 日志级别过滤（Debug级别被过滤）
 * 2. 日志文件轮转（达到大小限制后自动轮转）
 * 3. 日志内容正确写入文件
 * 
 * 测试使用临时目录模拟日志文件环境。
 */

#include "mq/core/logger.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

/**
 * @brief 辅助函数：读取文件内容为字符串
 * @param path 文件路径
 * @return 文件内容字符串
 */
std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

/**
 * @brief 测试日志级别过滤和文件轮转功能
 */
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

/**
 * @brief 主函数，运行所有日志系统单元测试
 * @return 0 表示测试成功
 */
int main() {
  FiltersAndRotates();
  return 0;
}
