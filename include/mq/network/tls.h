#pragma once

#include <cstddef>
#include <memory>
#include <string>

namespace mq::network {

/**
 * @brief TLS 配置选项
 *
 * 控制 TLS 连接的行为，包括证书、密钥和验证选项。
 *
 * 设计要点：
 * - 可选启用：enabled=false 时使用普通 TCP
 * - 双向认证：可选验证客户端证书
 * - 灵活验证：可配置是否验证对端证书
 *
 * 使用场景：
 * - TcpServer 和 TcpClient 的 TLS 配置
 * - 生产环境必须启用 TLS 保证数据安全
 *
 * 注意事项：
 * - 证书文件路径必须是绝对路径或相对于工作目录
 * - 私钥文件必须与证书匹配
 * - CA 文件用于验证对端证书，可以为空（使用系统默认）
 */
struct TlsOptions {
  /**
   * @brief 是否启用 TLS
   * 默认 false，即使用普通 TCP
   */
  bool enabled = false;

  /**
   * @brief 证书文件路径
   * PEM 格式，包含服务器/客户端证书
   */
  std::string certificate_file;

  /**
   * @brief 私钥文件路径
   * PEM 格式，与证书匹配
   */
  std::string private_key_file;

  /**
   * @brief CA 证书文件路径
   * 用于验证对端证书，为空则使用系统默认
   */
  std::string ca_file;

  /**
   * @brief 服务器名称
   * 用于 SNI（Server Name Indication）和证书验证
   */
  std::string server_name;

  /**
   * @brief 是否验证对端证书
   * 默认 true，生产环境必须启用
   */
  bool verify_peer = true;

  /**
   * @brief 是否要求客户端证书
   * 默认 false，启用后实现双向认证
   */
  bool require_client_certificate = false;
};

/**
 * @brief TLS IO 操作结果
 *
 * 表示 TLS 读写操作的状态，用于非阻塞 IO 的状态机。
 */
enum class TlsIoResult {
  kOk,        ///< 操作成功
  kWantRead,  ///< 需要读取更多数据（握手未完成）
  kWantWrite, ///< 需要写入更多数据（握手未完成）
  kClosed,    ///< 连接已关闭
  kError      ///< 发生错误
};

/**
 * @brief TLS 句柄类型
 * 使用 void* 避免暴露 OpenSSL 类型
 */
using TlsHandle = void*;

/**
 * @brief TLS 会话上下文
 *
 * 封装 OpenSSL 句柄，业务层只处理非阻塞 I/O 状态，不直接依赖 OpenSSL 类型。
 *
 * 设计要点：
 * 1. 封装 OpenSSL：隐藏底层实现细节
 * 2. 非阻塞 IO：返回状态码而非阻塞等待
 * 3. 会话管理：支持创建、握手、读写、关闭
 *
 * 在整体架构中的角色：
 * - TcpConnection 使用本类进行 TLS 握手和加密传输
 * - 与 EventLoop 配合，实现非阻塞 TLS
 *
 * 与其他模块的关系：
 * - TcpConnection 调用本类进行 TLS 操作
 * - TlsOptions 配置本类的行为
 *
 * 线程安全性：
 * - 单个会话（TlsHandle）只能单线程使用
 * - 上下文（TlsSessionContext）可多线程创建会话
 *
 * OpenSSL 集成：
 * - 使用 OpenSSL 的非阻塞模式
 * - 通过 BIO 实现内存缓冲
 * - 支持 TLS 1.2 和 TLS 1.3
 */
class TlsSessionContext {
 public:
  /**
   * @brief 析构函数，释放 OpenSSL 上下文
   */
  ~TlsSessionContext();

  // 禁用拷贝，OpenSSL 上下文不可复制
  TlsSessionContext(const TlsSessionContext&) = delete;
  TlsSessionContext& operator=(const TlsSessionContext&) = delete;

  /**
   * @brief 创建客户端 TLS 上下文
   *
   * @param options TLS 配置选项
   * @param error 可选的错误信息输出参数
   * @return 成功返回上下文对象，失败返回 nullptr
   */
  static std::unique_ptr<TlsSessionContext> CreateClient(const TlsOptions& options,
                                                         std::string* error);

  /**
   * @brief 创建服务器 TLS 上下文
   *
   * @param options TLS 配置选项
   * @param error 可选的错误信息输出参数
   * @return 成功返回上下文对象，失败返回 nullptr
   */
  static std::unique_ptr<TlsSessionContext> CreateServer(const TlsOptions& options,
                                                         std::string* error);

  /**
   * @brief 创建 TLS 会话
   *
   * @param socket 已建立的 TCP socket
   * @param server 是否为服务器模式
   * @param error 可选的错误信息输出参数
   * @return 成功返回 TLS 句柄，失败返回 nullptr
   */
  TlsHandle NewSession(int socket, bool server, std::string* error);

  /**
   * @brief 执行 TLS 握手
   *
   * 非阻塞操作，可能返回 kWantRead 或 kWantWrite。
   * 需要配合 EventLoop 的事件驱动完成握手。
   *
   * @param session TLS 会话句柄
   * @param error 可选的错误信息输出参数
   * @return 握手状态
   */
  TlsIoResult Handshake(TlsHandle session, std::string* error);

  /**
   * @brief TLS 读取数据
   *
   * 非阻塞操作，可能返回 kWantRead 表示需要更多数据。
   *
   * @param session TLS 会话句柄
   * @param buffer 接收缓冲区
   * @param capacity 缓冲区容量
   * @param size 输出参数，实际读取的字节数
   * @param error 可选的错误信息输出参数
   * @return 读取状态
   */
  TlsIoResult Read(TlsHandle session, void* buffer, std::size_t capacity, std::size_t* size,
                   std::string* error);

  /**
   * @brief TLS 写入数据
   *
   * 非阻塞操作，可能返回 kWantWrite 表示需要发送缓冲区。
   *
   * @param session TLS 会话句柄
   * @param buffer 要发送的数据
   * @param size 数据长度
   * @param written 输出参数，实际写入的字节数
   * @param error 可选的错误信息输出参数
   * @return 写入状态
   */
  TlsIoResult Write(TlsHandle session, const void* buffer, std::size_t size, std::size_t* written,
                    std::string* error);

  /**
   * @brief 关闭 TLS 会话
   *
   * 发送 close_notify 并释放会话资源。
   *
   * @param session TLS 会话句柄
   */
  void CloseSession(TlsHandle session);

 private:
  /**
   * @brief 私有构造函数
   *
   * @param context OpenSSL 上下文指针
   * @param server_name 服务器名称（用于 SNI）
   */
  explicit TlsSessionContext(void* context, std::string server_name);

  /**
   * @brief OpenSSL 上下文指针
   * 使用 void* 避免暴露 OpenSSL 类型
   */
  void* context_ = nullptr;

  /**
   * @brief 服务器名称
   */
  std::string server_name_;
};

}  // namespace mq::network