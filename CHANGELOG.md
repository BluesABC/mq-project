# 变更日志

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。
版本号按 `vMAJOR.MINOR.PATCH` 递增（见 `AGENTS.md` §5.27）。

## [Unreleased]

### P2 高可用与 Linux 验证
- 增加任期、投票、多数派提交索引和网络分区写入保护。
- 增加复制心跳、低任期请求拒绝、Follower 失联选举和客户端多 endpoint 故障切换。
- Windows Debug CTest 10/10、WSL2 Linux CTest 10/10、WSL2 ASan/UBSan CTest 10/10 通过。
- WSL2 真实 TCP 冒烟完成 1000 条消息生产并验证 Broker 优雅退出。

### P3 可观测性基础
- 新增 `METRICS(0x04)` 只读协议命令，返回 Prometheus 文本指标。
- Broker 统计请求、生产、拉取、错误及复制任期、提交索引和角色。
- 新增 Broker 指标单测，更新部署与架构文档。

### P3 生产保护
- 新增 `produce_rate_limit` 配置，按秒限制 PRODUCE/PRODUCE_BATCH 请求。
- 超过限制返回 `RATE_LIMITED`，复制内部请求不受影响。
- 新增限流单测。

### P3 Topic 配额
- 新增 `topic_produce_quota_bytes` 配置，按 Topic 每秒限制 key/value 生产字节数，默认 `0` 表示关闭。
- 超过配额返回 `QUOTA_EXCEEDED`；批量请求在写入前一次性检查，避免产生部分批次。
- `METRICS` 新增配额配置和当前窗口聚合使用量指标，并增加对应单测。

### P3 管理工具
- 新增 `mq_admin topics`，查询 Topic 名称与分区数。
- 新增 `mq_admin metrics`，输出 Prometheus 文本指标；复用 SDK 超时、重连和协议编解码。
- Windows、WSL2 Linux 及 WSL2 ASan/UBSan CTest 均通过，新增管理工具帮助命令测试。

### P3 稳定性验证
- 增加 CRC 损坏尾部和半条 WAL 记录恢复测试，增加消费者位点快照独立性测试。
- 增加客户端顺序消费及 `ack=0`、`ack=1`、`ack=all` 语义测试。
- 修复连接关闭后异步响应仍访问已释放 `MemoryPool` 的生命周期竞态；Windows、WSL2 Linux、WSL2 ASan/UBSan CTest 均为 11/11 通过。

### P3 性能回归门禁
- 新增 `tools/check_bench_matrix.sh`，校验矩阵场景、重复轮次、TPS 和 p99 阈值。
- 增加历史 WSL2 结果的保守基线与 Linux CTest fixture；Linux 普通和 sanitizer CTest 均为 12/12 通过。

### P3 消费基准与大响应修复
- Broker Fetch 响应按协议 payload 上限裁剪，TcpConnection 写队列支持受容量限制的完整大帧，修复 256B 消息多分区消费在约 64KiB 响应处超时。
- bench 消费读取 Topic 实际分区数，批量提交消费位点并在结束时补交最后位点；每次 bench 进程使用唯一 Producer ID，避免跨场景幂等缓存误命中。
- Windows Debug CTest 11/11、WSL2 Linux CTest 12/12、WSL2 ASan/UBSan CTest 12/12 通过；Windows/WSL2 1,000 条 TCP 生产消费验证通过。

### 测试构建修复
- Release 构建的 CTest 目标强制取消 `NDEBUG`，避免测试断言被编译掉后继续使用无效数据。
- 修复 VM16 Linux Release 下 `mq_core_tests` 因空协议帧触发 `std::out_of_range` 的问题。

### 项目状态
- P1 基础链路、P2 复制与故障切换已落地；P3 已开始接入可观测性基础能力。

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
