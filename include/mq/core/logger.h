#pragma once

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string_view>

namespace mq::core {

/**
 * @brief 日志级别枚举
 *
 * 日志接口集中处理级别过滤和文件轮转，业务代码不应自行写日志文件。
 *
 * 级别说明：
 * - kTrace：最详细的跟踪信息，仅用于开发调试
 * - kDebug：调试信息，比 Trace 粗粒度
 * - kInfo：一般信息，记录系统运行状态
 * - kWarn：警告信息，不影响运行但需要关注
 * - kError：错误信息，部分功能受影响
 * - kCritical：严重错误，系统无法继续运行
 */
enum class LogLevel {
  kTrace,     ///< 跟踪
  kDebug,     ///< 调试
  kInfo,      ///< 信息
  kWarn,      ///< 警告
  kError,     ///< 错误
  kCritical,  ///< 严重
};

/**
 * @brief 日志管理器
 *
 * 提供统一的日志接口，支持级别过滤和文件轮转。
 *
 * 设计要点：
 * 1. 单例模式：全局唯一实例
 * 2. 线程安全：内部使用互斥锁保护
 * 3. 文件轮转：自动按大小轮转日志文件
 * 4. 级别过滤：只输出不低于指定级别的日志
 *
 * 使用场景：
 * - 系统启动和关闭信息
 * - 错误和异常记录
 * - 性能指标和统计
 *
 * 使用示例：
 * @code
 * // 设置日志级别
 * Logger::Instance().SetLevel(LogLevel::kDebug);
 *
 * // 输出日志
 * Logger::Instance().Log(LogLevel::kInfo, "Server started");
 * Logger::Instance().Log(LogLevel::kError, "Connection failed");
 * @endcode
 *
 * 注意事项：
 * - 热路径禁止打印日志，避免影响性能
 * - 禁止打印消息体明文、密钥、完整凭据
 * - 允许打印 request_id、topic、partition、offset 等元信息
 *
 * 文件轮转：
 * - 当日志文件超过 max_file_bytes 时自动轮转
 * - 旧文件会被重命名为 .1、.2 等
 * - 最多保留 max_file_bytes * 2 的空间
 */
class Logger {
 public:
  /**
   * @brief 获取单例实例
   *
   * @return Logger 引用
   */
  static Logger& Instance();

  /**
   * @brief 设置日志级别
   *
   * 只有不低于此级别的日志才会输出。
   *
   * @param level 日志级别
   */
  void SetLevel(LogLevel level);

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
  bool SetFile(std::filesystem::path path, std::size_t max_file_bytes,
               std::string* error = nullptr);

  /**
   * @brief 关闭日志文件
   *
   * 在程序关闭前调用，确保所有日志都已写入。
   */
  void CloseFile();

  /**
   * @brief 输出日志
   *
   * @param level 日志级别
   * @param message 日志内容
   */
  void Log(LogLevel level, std::string_view message);

 private:
  /**
   * @brief 私有构造函数（单例模式）
   */
  Logger() = default;

  /**
   * @brief 检查并执行文件轮转
   *
   * @param next_line_bytes 下一行日志的预估大小
   */
  bool RotateIfNeeded(std::size_t next_line_bytes);

  /**
   * @brief 获取日志级别名称
   *
   * @param level 日志级别
   * @return 级别名称字符串
   */
  static std::string_view LevelName(LogLevel level);

  // ==================== 成员变量 ====================

  std::mutex mutex_;  ///< 互斥锁，保护并发访问

  LogLevel level_ = LogLevel::kInfo;  ///< 当前日志级别

  std::filesystem::path file_path_;  ///< 日志文件路径
  std::ofstream file_;               ///< 日志文件流

  std::size_t max_file_bytes_ = 0;  ///< 单个文件最大大小
  std::size_t file_bytes_ = 0;      ///< 当前文件已写入的字节数
};

}  // namespace mq::core