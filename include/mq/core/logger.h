#pragma once

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string_view>

namespace mq::core {

enum class LogLevel {
  kTrace,
  kDebug,
  kInfo,
  kWarn,
  kError,
  kCritical,
};

class Logger {
 public:
  static Logger& Instance();

  void SetLevel(LogLevel level);
  bool SetFile(std::filesystem::path path, std::size_t max_file_bytes, std::string* error = nullptr);
  void CloseFile();
  void Log(LogLevel level, std::string_view message);

 private:
  Logger() = default;
  bool RotateIfNeeded(std::size_t next_line_bytes);
  static std::string_view LevelName(LogLevel level);

  std::mutex mutex_;
  LogLevel level_ = LogLevel::kInfo;
  std::filesystem::path file_path_;
  std::ofstream file_;
  std::size_t max_file_bytes_ = 0;
  std::size_t file_bytes_ = 0;
};

}  // namespace mq::core
