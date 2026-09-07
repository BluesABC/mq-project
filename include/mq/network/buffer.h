#pragma once

#include <cstddef>
#include <string_view>

#include "mq/core/memory_pool.h"

namespace mq::network {

/**
 * @brief 网络缓冲区
 *
 * 从 MemoryPool 获取固定容量，避免收包过程中反复分配。
 *
 * 设计要点：
 * 1. 固定容量：避免热路径扩容
 * 2. 读写分离：维护读位置和写位置
 * 3. 自动紧凑：空间不足时移动数据，提高利用率
 *
 * 使用场景：
 * - TcpConnection 的读缓冲区
 * - 协议解码的输入缓冲
 *
 * 与其他模块的关系：
 * - 从 MemoryPool 分配内存，避免频繁 new/delete
 * - 被 TcpConnection 使用，管理接收的数据
 *
 * 线程安全性：
 * - 非线程安全，应单线程使用
 * - 通常与 EventLoop 的单线程模型配合
 *
 * 内存布局：
 * ```
 * [已消费][待处理数据][可写空间]
 *        ^           ^
 *    read_pos    write_pos
 * ```
 */
class Buffer {
 public:
  /**
   * @brief 构造函数，从内存池分配缓冲区
   *
   * @param pool 内存池，用于分配内存
   * @param capacity_bytes 缓冲区容量（字节）
   */
  Buffer(core::MemoryPool* pool, std::size_t capacity_bytes);

  /**
   * @brief 追加数据到缓冲区
   *
   * 如果空间不足，会自动紧凑（移动数据到头部）。
   * 如果仍然不足，返回 false（不会扩展）。
   *
   * @param data 要追加的数据
   * @return true 追加成功；false 空间不足
   */
  bool Append(std::string_view data);

  /**
   * @brief 获取可读的数据
   *
   * 返回待处理数据的字符串视图，不移动读指针。
   *
   * @return 可读数据的字符串视图
   */
  std::string_view Readable() const;

  /**
   * @brief 消费数据
   *
   * 消费后只移动读指针，不释放内存。
   * 空间不足时由 Append 触发紧凑整理。
   *
   * @param bytes 要消费的字节数
   */
  void Consume(std::size_t bytes);

  /**
   * @brief 清空缓冲区
   *
   * 重置读写指针到初始位置，不释放内存。
   */
  void Clear();

  /**
   * @brief 获取可读的字节数
   * @return 待处理数据的长度（字节）
   */
  std::size_t readable_bytes() const {
    return write_position_ - read_position_;
  }

  /**
   * @brief 获取可写的字节数
   * @return 剩余可用空间（字节）
   */
  std::size_t writable_bytes() const {
    return capacity_bytes_ - write_position_;
  }

 private:
  /**
   * @brief 紧凑缓冲区
   *
   * 将数据移动到缓冲区头部，释放已消费的空间。
   * 在 Append 空间不足时自动调用。
   */
  void Compact();

  /**
   * @brief 缓冲区数据指针
   */
  char* data_ = nullptr;

  /**
   * @brief 缓冲区总容量（字节）
   */
  std::size_t capacity_bytes_ = 0;

  /**
   * @brief 读指针位置
   * 指向下一条待消费的数据
   */
  std::size_t read_position_ = 0;

  /**
   * @brief 写指针位置
   * 指向下一个可写入的位置
   */
  std::size_t write_position_ = 0;
};

}  // namespace mq::network