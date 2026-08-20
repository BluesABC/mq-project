#include "mq/core/logger.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace mq::core {
namespace {

constexpr std::size_t kMaxLineBytes = 4096;

}  // namespace

Logger& Logger::Instance() {
  static Logger logger;
  return logger;
}

void Logger::SetLevel(LogLevel level) {
  std::lock_guard lock(mutex_);
  level_ = level;
}

bool Logger::SetFile(std::filesystem::path path, std::size_t max_file_bytes, std::string* error) {
  if (path.empty() || max_file_bytes == 0) {
    if (error != nullptr) *error = "invalid log file configuration";
    return false;
  }
  std::lock_guard lock(mutex_);
  std::error_code ec;
  if (!path.parent_path().empty()) std::filesystem::create_directories(path.parent_path(), ec);
  if (ec) {
    if (error != nullptr) *error = ec.message();
    return false;
  }
  file_.close();
  file_.open(path, std::ios::out | std::ios::app);
  if (!file_) {
    if (error != nullptr) *error = "cannot open log file";
    return false;
  }
  file_path_ = std::move(path);
  max_file_bytes_ = max_file_bytes;
  file_bytes_ = std::filesystem::exists(file_path_) ? std::filesystem::file_size(file_path_, ec) : 0;
  if (ec) file_bytes_ = 0;
  return true;
}

void Logger::CloseFile() {
  std::lock_guard lock(mutex_);
  file_.close();
  file_path_.clear();
  file_bytes_ = 0;
  max_file_bytes_ = 0;
}

void Logger::Log(LogLevel level, std::string_view message) {
  std::lock_guard lock(mutex_);
  if (level < level_) return;
  const auto now = std::chrono::system_clock::now();
  const auto time = std::chrono::system_clock::to_time_t(now);
  std::tm local_time{};
#ifdef _WIN32
  localtime_s(&local_time, &time);
#else
  localtime_r(&time, &local_time);
#endif
  std::ostringstream line;
  line << std::put_time(&local_time, "%Y-%m-%dT%H:%M:%S") << " [" << LevelName(level) << "] ";
  const std::size_t available = kMaxLineBytes > line.str().size() ? kMaxLineBytes - line.str().size() : 0;
  line << message.substr(0, available) << '\n';
  const std::string formatted = line.str();
  std::cerr << formatted;
  if (!file_) return;
  if (!RotateIfNeeded(formatted.size())) return;
  file_ << formatted;
  file_.flush();
  file_bytes_ += formatted.size();
}

bool Logger::RotateIfNeeded(std::size_t next_line_bytes) {
  if (file_bytes_ == 0 || next_line_bytes <= max_file_bytes_ - file_bytes_) return true;
  file_.close();
  std::error_code ec;
  const std::filesystem::path backup = file_path_.string() + ".1";
  std::filesystem::remove(backup, ec);
  ec.clear();
  std::filesystem::rename(file_path_, backup, ec);
  if (ec) return false;
  file_.open(file_path_, std::ios::out | std::ios::trunc);
  file_bytes_ = 0;
  return static_cast<bool>(file_);
}

std::string_view Logger::LevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace: return "trace";
    case LogLevel::kDebug: return "debug";
    case LogLevel::kInfo: return "info";
    case LogLevel::kWarn: return "warn";
    case LogLevel::kError: return "error";
    case LogLevel::kCritical: return "critical";
  }
  return "unknown";
}

}  // namespace mq::core
