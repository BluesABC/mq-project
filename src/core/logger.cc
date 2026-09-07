#include "mq/core/logger.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace mq::core {
namespace {

/**
 * @brief 单行日志最大字节数
 *
 * 防止超长日志导致内存问题。
 * 4096 字节足以容纳大多数日志行。
 */
constexpr std::size_t kMaxLineBytes = 4096;

}  // namespace

/**
 * @brief 获取单例实例
 *
 * 使用 Meyer's Singleton，线程安全且无需手动释放。
 *
 * @return Logger 引用
 */
Logger& Logger::Instance() {
  static Logger logger;
  return logger;
}

/**
 * @brief 设置日志级别
 *
 * 只有不低于此级别的日志才会输出。
 *
 * @param level 日志级别
 */
void Logger::SetLevel(LogLevel level) {
  std::lock_guard lock(mutex_);
  level_ = level;
}

/**
 * @brief 设置日志文件
 *
 * 启用文件输出，并配置轮转策略。
 *
 * @param path 日志文件路径
 * @param max_file_bytes 单个文件最大大小（字节），超过后轮转
 * @param error 可选的错误信息输出参数
 * @return true 设置成功；false 文件打开失败
 */
bool Logger::SetFile(std::filesystem::path path, std::size_t max_file_bytes, std::string* error) {
  // 参数校验
  if (path.empty() || max_file_bytes == 0) {
    if (error != nullptr) *error = "invalid log file configuration";
    return false;
  }

  std::lock_guard lock(mutex_);

  // 创建父目录（如果不存在）
  std::error_code ec;
  if (!path.parent_path().empty()) std::filesystem::create_directories(path.parent_path(), ec);
  if (ec) {
    if (error != nullptr) *error = ec.message();
    return false;
  }

  // 关闭旧文件，打开新文件（追加模式）
  file_.close();
  file_.open(path, std::ios::out | std::ios::app);
  if (!file_) {
    if (error != nullptr) *error = "cannot open log file";
    return false;
  }

  // 记录文件信息
  file_path_ = std::move(path);
  max_file_bytes_ = max_file_bytes;

  // 获取当前文件大小（如果文件已存在）
  file_bytes_ =
      std::filesystem::exists(file_path_) ? std::filesystem::file_size(file_path_, ec) : 0;
  if (ec) file_bytes_ = 0;

  return true;
}

/**
 * @brief 关闭日志文件
 *
 * 在程序关闭前调用，确保所有日志都已写入。
 */
void Logger::CloseFile() {
  std::lock_guard lock(mutex_);
  file_.close();
  file_path_.clear();
  file_bytes_ = 0;
  max_file_bytes_ = 0;
}

/**
 * @brief 输出日志
 *
 * 日志格式：`YYYY-MM-DDTHH:MM:SS [level] message`
 *
 * @param level 日志级别
 * @param message 日志内容
 */
void Logger::Log(LogLevel level, std::string_view message) {
  std::lock_guard lock(mutex_);

  // 级别过滤：低于当前级别的日志不输出
  if (level < level_) return;

  // 获取当前时间
  const auto now = std::chrono::system_clock::now();
  const auto time = std::chrono::system_clock::to_time_t(now);
  std::tm local_time{};
#ifdef _WIN32
  localtime_s(&local_time, &time);
#else
  localtime_r(&time, &local_time);
#endif

  // 格式化日志行：时间 + 级别 + 消息
  std::ostringstream line;
  line << std::put_time(&local_time, "%Y-%m-%dT%H:%M:%S") << " [" << LevelName(level) << "] ";

  // 截断超长消息
  const std::size_t available =
      kMaxLineBytes > line.str().size() ? kMaxLineBytes - line.str().size() : 0;
  line << message.substr(0, available) << '\n';

  const std::string formatted = line.str();

  // 输出到标准错误
  std::cerr << formatted;

  // 如果没有配置文件，直接返回
  if (!file_) return;

  // 检查是否需要轮转文件
  if (!RotateIfNeeded(formatted.size())) return;

  // 写入文件
  file_ << formatted;
  file_.flush();
  file_bytes_ += formatted.size();
}

/**
 * @brief 检查并执行文件轮转
 *
 * 当文件超过 max_file_bytes_ 时，将当前文件重命名为 .1，
 * 然后创建新的空文件继续写入。
 *
 * @param next_line_bytes 下一行日志的预估大小
 * @return true 继续写入；false 轮转失败
 */
bool Logger::RotateIfNeeded(std::size_t next_line_bytes) {
  // 如果文件为空或空间足够，直接返回
  if (file_bytes_ == 0 || next_line_bytes <= max_file_bytes_ - file_bytes_) return true;

  // 关闭当前文件
  file_.close();

  // 轮转：重命名为 .1
  std::error_code ec;
  const std::filesystem::path backup = file_path_.string() + ".1";
  std::filesystem::remove(backup, ec);  // 删除旧的备份
  ec.clear();
  std::filesystem::rename(file_path_, backup, ec);  // 重命名当前文件
  if (ec) return false;

  // 创建新的空文件
  file_.open(file_path_, std::ios::out | std::ios::trunc);
  file_bytes_ = 0;

  return static_cast<bool>(file_);
}

/**
 * @brief 获取日志级别名称
 *
 * @param level 日志级别
 * @return 级别名称字符串
 */
std::string_view Logger::LevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return "trace";
    case LogLevel::kDebug:
      return "debug";
    case LogLevel::kInfo:
      return "info";
    case LogLevel::kWarn:
      return "warn";
    case LogLevel::kError:
      return "error";
    case LogLevel::kCritical:
      return "critical";
  }
  return "unknown";
}

}  // namespace mq::core