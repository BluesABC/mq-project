# mq-project 架构设计文档

> 版本: v0.1（骨架阶段） · 更新: 2026-08-19
> 各模块内部实现细节（存储引擎 WAL+mmap、网络层 epoll/io_uring、并发模型、内存管理）见 **`docs/design-details.md`**。

## 1. 设计目标与约束

| 维度 | 目标 |
|------|------|
| 高并发 | 单 Broker 支撑 ≥ 10w TPS 生产 + 消费，连接数 ≥ 5w |
| 高可用 | 多副本复制，节点故障自动切换，消息零丢失（at-least-once） |
| 低延迟 | 生产 p99 < 5ms，消费 p99 < 10ms（同机内网） |
| 持久化 | 磁盘 WAL 全持久化，fsync 策略可配置 |
| 语言/标准 | C++17（兼容性优先），Linux 主生产平台 |
| 依赖 | 最小依赖：spdlog + gtest，其余全部自研 |

## 2. 总体架构

```
                        ┌─────────────────────────────────────────────┐
                        │                Broker 进程                    │
                        │                                             │
   Producer ──TCP──▶   │  ┌─────────────┐      ┌──────────────────┐   │
   Consumer ──TCP──▶   │  │  网络层       │      │   核心逻辑层       │   │
                        │  │  Main Reactor │────▶│  Queue Manager    │   │
                        │  │  (accept)     │     │  (路由/元数据)     │   │
                        │  │      │        │     └────────┬─────────┘   │
                        │  │  Sub Reactor×N│              │             │
                        │  │  (epoll IO)  │──▶ Worker 线程池 (编解码/分发)│
                        │  └─────────────┘      └──────────────────┘   │
                        │                              │               │
                        │                     ┌────────▼─────────┐     │
                        │                     │   存储引擎         │     │
                        │                     │  Segment + WAL    │     │
                        │                     └──────────────────┘     │
                        └─────────────────────────────────────────────┘
                                      │ 复制 (后续阶段)
                              ┌────────▼────────┐
                              │  Follower Broker │
                              └─────────────────┘
```

### 2.1 分层职责

| 层 | 模块 | 职责 |
|----|------|------|
| 网络层 | `src/network` | 连接生命周期管理、epoll 事件分发、粘包/半包处理 |
| 协议层 | `src/protocol` | 二进制协议定义、编解码、命令/状态码 |
| 核心层 | `src/core` | 队列路由、存储引擎、线程池、并发原语 |
| 服务端 | `src/server` | Broker 装配、请求分发 Handler、配置加载、主入口 |
| 客户端 | `src/client` | SDK：同步/异步 API、连接池、重试 |

## 3. 线程模型（多线程 Reactor）

```
Main Reactor ──accept──▶ 分配连接
      │
      └─▶ Sub Reactor 0 ─┐
      └─▶ Sub Reactor 1 ─┤ 每个 Sub Reactor 持有 epoll 实例 + 连接集合，
      └─▶ ...           ─┤ 独立线程，负责该连接集合的读/写事件
      └─▶ Sub Reactor N ─┘

      读事件 ──▶ 协议解码 ──▶ 任务队列 ──▶ Worker 线程池 ──▶ Queue Manager
      写事件 ◀── 响应队列 ◀────────────────────────┘
```

- **Main Reactor**（1 线程）：`listen` + `accept`，round-robin 将新连接分配给 Sub Reactor。
- **Sub Reactor**（默认 N = CPU 核数）：每个线程一个 epoll，采用 **ET 模式 + 事件分片**，同一连接的所有事件固定在同一个 Sub Reactor 线程上处理（避免连接加锁）。
- **Worker 线程池**（默认 W = CPU 核数 × 2）：无锁 MPMC 任务队列（RingBuffer），执行实际业务逻辑（写入存储、拉取消息、消费位点管理）。
- 跨线程通信通过 **每个 Sub Reactor 一个写队列** 完成：Worker 产出的响应放入对应连接所在 Reactor 的写队列，由该 Reactor 线程触发 `EPOLLOUT` 写出。

### 关键并发原语
- `Mutex`/`CondVar`：低频元数据（队列注册、消费者组）。
- 无锁环形队列：`core/buffer.h` 提供 SPSC/MPMC 两种 RingBuffer。
- 细粒度锁：存储引擎按 **队列分区** 分片加锁，互不阻塞。

## 4. 存储引擎设计（WAL + Segment）

数据目录布局：
```
$data_dir/
  ├── metadata/                     # 队列/消费者组元数据（JSON，启动时加载）
  │   ├── queue.json
  │   └── consumer_group.json
  └── queues/
      └── <topic>/
          └── <partition>/
              ├── 00000000000000000000.log    # 消息日志段（默认 1GB 滚动）
              ├── 00000000000000000000.index  # 稀疏索引（offset → 段内位置）
              └── log_start_offset            # 已清理起点（配合过期策略）
```

### 4.1 日志段格式

```
段文件 = 记录序列
记录 = [crc32(4) | payload_len(4) | payload]
       payload = [offset(8) | timestamp(8) | key_len(2) | key | value_len(4) | value]

offset 为全局递增逻辑偏移（相对 partition 首条消息，从 0 开始）。
索引文件：每写 4096 条消息或在 mmap 索引中记录 (offset, file_pos) 一条稀疏项。
```

### 4.2 写路径
1. 追加记录到当前活跃段（`pwrite`，先写 `.log`）。
2. 批量更新 mmap 索引。
3. 按 `fsync_policy` 落盘：
   - `EVERY_MSG`：每条 fsync（最安全，吞吐最低）；
   - `EVERY_N_MS`（默认 5ms）：定时批量 fsync（推荐）；
   - `EVERY_N_BYTES`：每 N MB fsync。
4. 通过 **写入者单线程化**（per-partition write lock 或专用写线程）保证顺序追加。

### 4.3 读路径
- 消费者按 `offset` 定位：二分查稀疏索引 → 定位段文件 + 文件内位置 → 顺序批量读。
- 跨越段边界自动回卷到下一个 `.log`。

### 4.4 过期清理
- 基于 `log_start_offset` 推进，后台线程定期将过期/超限段标记删除，目录恢复。

## 5. 消息协议

自定义二进制协议（length-prefixed framing），详见 `docs/api-spec.md`。

```
[magic:2][version:1][command:1][request_id:8][flags:2][topic_len:2][topic][payload_len:4][payload]
响应：[magic:2][version:1][status:1][request_id:8][flags:2][payload_len:4][payload]
```

核心命令：`CREATE_TOPIC`、`DELETE_TOPIC`、`PRODUCE`、`FETCH`、`COMMIT_OFFSET`、`LIST_TOPIC`、`HEARTBEAT`。

## 6. 高可用设计（复制，后续阶段）

- **主从复制**：Producer 只写 Leader；Follower 定时从 Leader 拉取日志段增量，追加到本地存储。
- **提交语义**：`PRODUCE` 的 ack 时机支持 `ack=0 / ack=1 / ack=all` 三档。
- **故障切换**：基于元数据服务/一致性算法选主，切换后消费位点从 `log_start_offset` 恢复。
- P2 已实现 `ReplicationCoordinator`、TCP `ReplicationClient`、内部复制 Fetch/Append、周期增量拉取、复制心跳和 quorum ack。当前选主为确定性健康节点选择，尚未达到 Raft/ZAB 的任期、多数派日志提交和网络分区安全保证。

## 7. 模块接口（骨架）

### 7.1 核心层 `include/mq/core`
| 头文件 | 内容 |
|--------|------|
| `buffer.h` | SPSC/MPMC 无锁环形队列 |
| `thread_pool.h` | 线程池抽象 |
| `storage_engine.h` | 存储引擎接口：append / read / recover |
| `queue_manager.h` | 队列注册与路由 |

### 7.2 网络层 `include/mq/network`
| 头文件 | 内容 |
|--------|------|
| `event_loop.h` | 事件循环抽象（epoll 封装） |
| `tcp_server.h` | 服务端 accept 与连接分发 |
| `tcp_connection.h` | 连接状态机与读写缓冲 |
| `protocol_codec.h` | 协议编解码 |

### 7.3 协议层 / 服务端 / 客户端
| 头文件 | 内容 |
|--------|------|
| `protocol/commands.h` | 命令与状态码枚举 |
| `server/broker.h` | Broker 装配入口 |
| `server/handler.h` | 命令分发 Handler |
| `client/client.h` | 客户端 SDK 接口 |

## 8. 构建与测试

- CMake ≥ 3.16，`-DMQ_BUILD_TESTS=ON` 构建单元测试与集成测试。
- 单元测试：`test/unit`（gtest），覆盖存储、编解码、环形队列。
- 压测：`bench`（吞吐/延迟报告），`tools` 提供运维脚本（topic 管理、数据目录检查）。

## 9. 里程碑（骨架 → MVP → 高可用）

| 阶段 | 范围 |
|------|------|
| P0 骨架 | 目录/构建/占位接口（本次） |
| P1 MVP | 单机持久化 Broker + 客户端，epoll Reactor，生产/消费全链路 |
| P2 高可用 | 主从复制、ack 语义、故障切换 |
| P3 生产化 | 监控、限流、配额、管理工具完善（进行中） |

### 9.1 P2 复制安全

复制协调器维护单调递增任期、投票者、Leader 角色和 commit index。写入仅由当前 Leader 接受；配置副本后，Leader 失去多数派心跳时拒绝对外生产。副本追加携带任期和 Leader 标识，低任期请求被拒绝；ack=all 只有本地日志和多数副本确认后推进提交索引。SDK 可配置多个 Broker endpoint，在连接失败时轮换并按指数退避重连。

### 9.2 P3 可观测性

Broker 通过 `METRICS(0x04)` 提供只读 Prometheus 文本指标，覆盖请求、生产、拉取、错误、复制任期、提交索引和当前角色。指标查询复用现有 TCP 协议和连接模型，不增加第三方依赖；生产部署可由外部 exporter 转发到 Prometheus。

Broker 支持按秒生产请求限流，默认值为 0（关闭）。限流在 Broker 入口执行，批量请求按请求计数，复制内部请求绕过该限制；超过窗口返回 `RATE_LIMITED`，由客户端退避重试。

Broker 支持按 Topic 的生产字节配额，默认值为 0（关闭）。配额窗口为 1 秒，统计新增消息的 key/value 字节；单条消息在 WAL 写入前检查，批量请求先完成整批解析和配额预留后再逐条写入，复制内部请求绕过配额。超限返回 `QUOTA_EXCEEDED`，并通过 `METRICS` 暴露配置值和当前窗口聚合用量。

P3 提供 `mq_admin` 只读管理工具：`topics` 复用 `LIST_TOPIC` 查询 Topic 元数据，`metrics` 复用 `METRICS` 输出 Prometheus 文本；工具使用 SDK 的连接超时和自动重连能力，不增加额外监听端口或第三方依赖。
