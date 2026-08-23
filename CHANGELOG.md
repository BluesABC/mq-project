# 变更日志

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。
版本号按 `vMAJOR.MINOR.PATCH` 递增（见 `AGENTS.md` §5.27）。

## [Unreleased]

### P2 高可用与 Linux 验证
- 增加任期、投票、多数派提交索引和网络分区写入保护。
- 增加复制心跳、低任期请求拒绝、Follower 失联选举和客户端多 endpoint 故障切换。
- Windows Debug CTest 10/10、WSL2 Linux CTest 10/10、WSL2 ASan/UBSan CTest 10/10 通过。
- WSL2 真实 TCP 冒烟完成 1000 条消息生产并验证 Broker 优雅退出。

### 项目状态
- 完成 P1 进度盘点：基础协议、存储、并发、Broker、Topic 元数据持久化、Windows TCP 验证与日志已落地；消费者组、Linux epoll、SDK、性能基线和高可用尚未完成。

### P1 Topic 元数据持久化
- 新增二进制 `topics.meta` 全量快照，保存 Topic 名称和分区数；创建、删除时通过临时文件与原子替换发布。
- Broker 启动时恢复 Topic 快照；新增 3 Topic 重启恢复测试，校验完整列表及分区数。

### P1 可观测性基础
- 新增线程安全 `mq::core::Logger`，支持级别过滤、stderr 输出和单文件滚动。

### P1 并发基础
- 新增固定容量 MPMC 队列，使用 acquire/release 内存序保障槽位发布和回收。
- 新增有界线程池，支持任务拒绝、优雅排空与关闭后拒绝提交。
- 增加 MPMC 多生产者/多消费者和线程池任务排空测试。

### P1 协议流处理
- 新增请求流式解码器，支持半包/粘包重组并限制累计缓冲和单帧长度。

### P1 Broker 核心
- 新增 Topic 元数据与稳定分区路由，以及同步 Broker 的创建 Topic、列出 Topic、生产和拉取处理链路。

### P1 网络基础
- 新增固定 owner 线程的 EventLoop，支持有界跨线程任务投递和关闭时排空。
- 新增 loop-local 内存池、固定容量网络缓冲和 TcpConnection 读写状态封装。
- EventLoop 新增 owner-thread idle 轮询回调，为 select/epoll socket 后端提供调度入口。
- 新增 Windows 非阻塞 Winsock `TcpServer`，完成 accept、流式解码、Worker 分发与响应回投。

### P1 基础能力
- 实现协议请求/响应帧编解码及 magic、版本、长度和 payload 上限校验。
- 实现单分区 WAL 追加、读取、CRC 校验和重启时损坏尾部截断恢复。
- 增加核心模块 CMake 测试目标。

### 骨架（P0）
- 建立仓库结构与 CMake 构建骨架。
- 文档体系：PRD、AGENTS、architecture、design-details、api-spec、test-plan、deployment。

### 规划中（P1 MVP）
- 存储引擎：WAL 段文件 + mmap 稀疏索引 + 崩溃恢复。
- 网络层：epoll Reactor（Main/Sub）+ 协议编解码 + 粘包处理。
- 并发：Worker 线程池 + 无锁 MPMC 队列。
- 核心：Topic/分区管理、生产/消费、位点提交。
- 客户端 SDK 与单测/集成测/压测。
