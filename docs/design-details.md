# mq-project 模块详细设计

> 版本: v0.1 · 更新: 2026-08-19
> 本文档描述各模块**内部实现细节**，是编码时的实现依据；架构总览见 `docs/architecture.md`。

## 目录

1. [存储引擎（WAL + mmap）](#1-存储引擎wal--mmap)
2. [网络层（epoll / io_uring）](#2-网络层epoll--io_uring)
3. [并发模型（线程池 + Lock-Free Queue）](#3-并发模型线程池--lock-free-queue)
4. [内存管理](#4-内存管理)
5. [模块间数据流与接口约定](#5-模块间数据流与接口约定)

---

## 1. 存储引擎（WAL + mmap）

### 1.1 目标与约束

- 写路径顺序追加，零随机写；读路径支持随机 offset 定位。
- 崩溃后最多丢 `fsync 窗口` 内的数据（可配置为 0）。
- 单分区串行写，多分区并行写（分片锁）。

### 1.2 目录与文件布局

```
<data_dir>/
  metadata/
    topics.meta            # Topic 全量快照（二进制，启动时加载）
    groups.json            # 消费组位点
  queues/
      <topic-hex>/              # Topic UTF-8 原始字节的十六进制编码
      <partition>/
        00000000000000000000.log     # 数据段（默认 1GiB 滚动）
        00000000000000000000.idx     # mmap 稀疏索引（默认 8B/条）
        log_start_offset             # 清理起点（字节文件）
```

### 1.2.1 Topic 元数据快照

`TopicMetadataStore` 将 Topic 与分区数保存到 `<data_dir>/metadata/topics.meta`。文件使用固定大端二进制布局，避免引入 JSON 解析依赖：

```
magic(4, "MQTM") | version(4, 1) | topic_count(4)
重复 topic_count 次：name_len(2) | name(UTF-8 原始字节) | partition_count(4)
```

- Topic 数最多 1024，名称最多 65535 字节；加载时校验 magic、版本、边界及 Topic 名合法性，非法或截断文件会导致 Broker 启动失败。
- 创建和删除 Topic 均在 Broker 的元数据互斥区内执行：先更新内存快照，再写入 `topics.meta.tmp`，关闭写流后以原子替换发布 `topics.meta`。Windows 使用 `MoveFileExW` 的 `REPLACE_EXISTING | WRITE_THROUGH`，POSIX 使用同目录 `rename`。
- 发布失败时 Broker 回滚本次内存变更，避免内存 Topic 列表与已落盘快照分歧。启动时先打开存储，再加载快照并整体替换 `QueueManager` 元数据。

### 1.3 日志段（.log）字节布局

段文件名 = 起始 offset（19 位十进制补零），从 0 开始递增。

```
┌─────────────────────────────────────────────────────────┐
│ segment = 若干 record 顺序追加，无记录级长度头以外的填充     │
│                                                         │
│ record:                                                  │
│   crc32(4)  |  body_len(4)  |  body                       │
│   body = offset(8) | timestamp(8) | key_len(2) | key     │
│        | value_len(4) | value                            │
│                                                         │
│   crc32 覆盖 body 全部字节；body_len 不含 crc 头          │
└─────────────────────────────────────────────────────────┘
```

- 记录跨段边界时不拆分：当前段剩余空间 < 单条记录大小时，整条写入下一段。
- 段内最后一条记录后允许存在**未写完的半条记录**（崩溃窗口），恢复时截断。

### 1.4 稀疏索引（.index）字节布局

- 每写入 `kIndexInterval`（默认 1000）条消息追加一条索引项。
- 条目固定 16B：`offset(8) | file_pos(8)`，用 `mmap` 映射，写后 `msync` 按需。
- 查找流程：二分索引 → 得 (offset, file_pos) 最近前驱 → 顺序扫描段内记录。
- 索引文件预分配（稀疏 mmap），随段滚动重建。

### 1.5 写路径（单写者）

```
Producer 请求 → Worker 线程 → PartitionWriter（per-partition 专用写线程）
                                   │
                     ┌─────────────▼─────────────┐
                     │  写缓冲 WriteBuffer (≤256KB) │
                     │  攒批 → pwrite(segment)     │
                     └─────────────┬─────────────┘
                                   │ 按策略 fsync：
                                   │  kEveryMessage / kEveryNMs(默认5ms)
                                   │  / kEveryNBytes(默认64MB)
                                   ▼
                              返回 offset + 通知 index updater
```

- **单写者模型**：每个分区一个写入者，天然串行追加，无需写锁。
- 批量 pwrite 后记录 `committed_offset`；读方只允许读 `≤ committed_offset` 的数据（避免读到未持久化记录）。

### 1.6 fsync 策略与 `flush`

| 策略 | 行为 | 用途 |
|------|------|------|
| `kEveryMessage` | 每条 pwrite 后 `fdatasync` | ack=all，零丢失 |
| `kEveryNMs` | 定时器（5ms）批量 `fdatasync` | 默认推荐，p99 低 |
| `kEveryNBytes` | 累计 64MB 后 `fdatasync` | 吞吐最大化 |

- 后台 `Flush` 线程负责定时 fsync + 触发 `committed_offset` 推进。
- ack 返回时机 = `committed_offset ≥ 该消息 offset` 时。

### 1.7 崩溃恢复（Recovery）

1. 启动扫描所有 `.log`，重建 segment 列表。
2. 从最后一个非空段尾部向前校验：`crc32` 失败 → 截断该记录及之后数据（半条窗口）。
3. 重建稀疏索引（重新扫一遍写索引）或基于 `.idx` 尾部校验后增量恢复。
4. 依据 `log_start_offset` 加载过期段列表（仅删除清理，不读入内存）。
5. 幂等表与消费组位点从 `metadata/` 加载。

### 1.8 段滚动与过期清理

- 当前段达到 `segment_size`（默认 64MiB）后，下一条完整记录写入新段；文件名为 20 位十进制 offset 段号加 `.log`，每段旁有同名 `.index`。
- 清理线程（1 个）遍历分区：删除 `end_offset < log_start_offset` 的段，随后更新目录元数据。
- 后台清理线程按 `retention.ms` 或 `retention.bytes` 删除最老的非活动段，始终保留当前段。

### 1.9 并发与缓存

- `partition → SegmentList` 全局表用 `shared_mutex` 保护（写少读多）。
- 热数据（最近写入段）LRU 缓存在内存（见 §4.3）。
- 读路径：定位后顺序预读（`posix_fadvise` / io_uring READV），批量返回。

---

## 2. 网络层（epoll / io_uring）

### 2.1 线程拓扑

```
Main Reactor（1 线程）         Sub Reactor × N（N = CPU 核数）
  listen+accept                 各自持有 epoll/io_uring fd
       │ round-robin 分配             │
       └─────────────► 连接集合 C0    └─► 连接集合 C1 ...
```

- 连接与 Reactor 线程绑定：一个连接的所有事件固定在同一线程处理，天然无锁。
- 每个 Sub Reactor 配一个 **写任务队列**（MPMC），Worker 的响应经此队列投递，由 Reactor 线程触发写。
- 当前 `EventLoop` 已提供固定 owner 线程与有界跨线程任务投递；socket 事件注册将在 `TcpServer`/`TcpConnection` 接入时叠加到该线程归属模型上。
- `EventLoop` 的 idle 回调在 owner 线程执行并以 5ms 等待窗口唤醒；Windows `select` 兜底和 Linux epoll 后端将通过该回调驱动，不得另起线程直接操作连接。
- Windows 当前实现使用非阻塞 Winsock + `select`：accept、连接表、流解码和 socket 写入均在 EventLoop owner 线程；完整请求投递 Worker，Worker 只调用 Handler 并通过 `TcpConnection::Send` 回投响应。

### 2.2 epoll 后端

- 使用 **ET（边缘触发）+ 非阻塞**：水平触发 + 多路复用时易惊群且重复通知。
- 事件处理：
  - `EPOLLIN`：循环 `read` 到 `EAGAIN`，数据入连接读缓冲，交给协议解码器。
  - `EPOLLOUT`：写缓冲非空时注册，写完清空后注销 `EPOLLOUT`（避免忙轮询）。
  - `EPOLLERR/EPOLLHUP`：关闭连接，归还连接对象到池（§4.2）。
- `epoll_ctl` 变更统一由持有该连接的 Reactor 线程执行，避免跨线程竞态。

### 2.3 io_uring 后端（可选，Linux ≥ 5.19）

- 每个 Sub Reactor 一个 `io_uring`（SQ/CQ 环）。
- 读路径：提交 `IORING_OP_READV` 到 SQ，完成回调在 CQ 侧处理。
- 写路径：`IORING_OP_WRITEV`，支持固定缓冲（`IORING_REGISTER_BUFFERS`）实现零拷贝。
- 与 epoll 的关系：io_uring 提供 `IORING_OP_POLL_ADD` 兼容 epoll 语义；抽象层 `EventLoop` 屏蔽差异，默认 epoll，编译期开关 `MQ_IO_URING`。

### 2.4 编解码与粘包处理

```
read 缓冲（默认 64KB 可增长） → 解析循环：
  1) 检查 header（magic+version+command+...）
  2) 依据 payload_len 判断是否收到完整帧
  3) 完整 → 交 Handler；不完整 → 保留残帧，等待下次 EPOLLIN
```

- 协议解码在 **Worker 线程**完成（Reactor 线程只做 read/write，不做业务解析，满足"网络层不做阻塞 IO"约束）。
- 解码产物（Request）通过 MPMC 队列投递到 Worker 池。

当前已实现 `TcpConnection` 的固定容量读写缓冲与 owner-loop 状态约束：Reactor 只调用 `OnReadable` 和消费已写字节，Worker 通过 `Send` 回投写任务。socket read/write 注册尚未接入。

### 2.5 连接状态机

```
       accept
         ▼
   ┌───────────┐  握手(可选)  ┌───────────┐
   │  INIT     │───────────▶ │ ESTABLISHED │
   └───────────┘             └─────┬─────┘
         ▲                         │ 空闲超时/对端关闭
         │                         ▼
         └──────── 回收对象到池 ◀── CLOSED
```

- 心跳：Reactor 线程维护定时器堆，30s 无活动则踢连接。
- 空闲超时可配置；连接对象复用见 §4.2。

### 2.6 流量控制与背压

- 读侧：连接读缓冲达到上限（默认 8MB）→ 暂不注册 `EPOLLIN`，向对端发送窗口暂停（预留协议位）。
- 写侧：写缓冲达到上限 → 通知上游 Producer 重试（响应含 `RETRY` 状态码）；避免无限缓存。

---

## 3. 并发模型（线程池 + Lock-Free Queue）

### 3.1 总体线程清单

| 线程 | 数量 | 职责 |
|------|------|------|
| Main Reactor | 1 | accept、连接分配 |
| Sub Reactor | N = 核数 | 连接 IO、定时器 |
| Worker Pool | W = 核数 × 2 | 协议解码、业务处理、存储调用 |
| Partition Writer | 分区数（懒创建） | 分区串行写入 + fsync |
| Flush | 1 | 定时 fsync、committed 推进 |
| Cleaner | 1 | 段过期清理 |
| (可选) Replication | 按副本数 | P2 阶段 |

### 3.2 线程池

- 任务接口：`void submit(Task)`，Task 为可移动的 `std::function`。
- 当前实现：固定数量 Worker + 有界 MPMC 全局任务队列 + 条件变量唤醒；`Submit` 在队列满或停止后返回失败，由调用方执行背压。
- 后续优化：支持 `steal()` 工作窃取（见 3.4），但不得改变有界队列与关闭时排空的语义。
- 当前拒绝策略：任务队列满或线程池停止时 `Submit` 返回 `false`，由调用方执行背压或返回重试状态。
- 线程名/亲和性：`pthread_setname_np` + `sched_setaffinity`（Linux）提升缓存局部性。

### 3.3 无锁 MPMC 环形队列

```
MPMCQueue<T>：固定容量（2 的幂）环形数组 + 三个原子槽位
   head（队首，生产者 CAS 推进）
   tail（队尾，消费者 CAS 推进）
   size 或 用 head/tail 差值判断满/空

   push：  循环 CAS 尝试占据槽位 → 写入数据 → release 标记可见
   pop：   循环 CAS 尝试取槽位 → 读取数据 → release 标记释放
```

- 伪共享防护：head/tail 槽位 `alignas(64)` 隔离到不同缓存行。
- 无界变体（任务队列）使用 `moodycamel` 风格的分段队列，或退化用带锁队列 + 背压。
- 内存序：`acquire/release` 配对保证"先数据后标记"，禁止用 `relaxed`。

### 3.4 工作窃取（Work Stealing）

- 每个 Worker 线程一个本地无锁双端队列（DEQueue）。
- 优先取本地队尾（LIFO，缓存友好）；本地空则随机/轮询窃取他人队首。
- 收益：任务不平衡（短连接多、长请求少）时吞吐提升，避免热线程饥饿。

### 3.5 跨线程通信约定

```
Worker 产出响应 ──► 目标连接所属 Sub Reactor 的写队列 ──► Reactor 触发 EPOLLOUT
Reactor 收到完整帧 ──► MPMC 任务队列 ──► Worker 池
```

- 明确禁止：Reactor 线程内同步等待 Worker；Worker 内直接操作 socket。
- 连接生命周期用 `shared_ptr<TcpConnection>`，避免异步回调中悬垂。

---

## 4. 内存管理

### 4.1 内存池

- 分片内存池（Arena/Slab），按大小类分桶：16/32/64/.../4096B，大对象走 `malloc`。
- 核心数据结构与消息体频繁分配/释放，走池化以降低分配器锁竞争。
- 每个 Worker 线程独立 Arena，线程退出时回收 → 无跨线程释放竞争。
- 扩容策略：固定块池不足时倍增扩容，池大小可配置（`memory.pool_size`）。

当前基础实现提供 owner-thread `MemoryPool` 和一次性分配的固定容量 `Buffer`，超限时直接失败以触发上层背压；Slab 分桶和对象复用将在连接管理接入后补齐。

### 4.2 对象池

- **连接对象池**：预分配 `max_connections`（默认 5w）个 `TcpConnection`，空闲链表管理。
- **消息对象池**：`Message` 及 payload 缓冲复用，配合零拷贝引用计数（见 4.3）。
- 对象归还要求：所有异步引用释放（引用计数归零）后才回池，防止 UAF。

### 4.3 缓冲与零拷贝

- 读/写缓冲：`Buffer` 基于 `iovec` 链，避免跨片拷贝。
- 消息从网络到磁盘再到网络：网络缓冲 → 分区写缓冲（拷贝一次，因需落盘校验）→ 读返回时引用 mmap 段内存（`mmap` 只读视图，零拷贝回发）。
- 引用计数：mmap 段对象 `shared_ptr`，读方持有期间段文件不被清理线程删除。
- 限制单连接最大缓存（读 8MB / 写 8MB），超限走背压。

### 4.4 内存上限与监控

| 项目 | 默认值 | 超限行为 |
|------|--------|----------|
| 写缓冲总量 | 512MB | 拒绝新写 + 触发 fsync |
| 连接缓存总量 | 按连接数 × 单连接上限 | 触发背压 |
| 内存池占用 | 2GB | 告警 + 降级为 malloc |
| 段 mmap 总量 | 活跃段仅映射窗口（64MB） | LRU 换出 |

- 周期性采样（`/proc/self/status` / `malloc_stats`）输出到日志与监控接口。

### 4.5 缓存一致性

- 写路径：写缓冲 → fsync → 更新 committed → 读路径才可见（release/acquire 配对）。
- mmap 索引更新后 `msync` 时机与 fsync 对齐，避免索引超前于数据。

---

## 5. 模块间数据流与接口约定

```
Client ──(帧)──▶ TCP Server ──▶ Reactor(read) ──▶ MPMC ──▶ Worker(解码)
                                                          │
                                          ┌───────────────┤
                                          ▼               ▼
                                     Queue Manager    [分组/位点]
                                          │
                                          ▼
                                     StorageEngine ──▶ PartitionWriter ──▶ 段文件
                                                          │ fsync
                                                          ▼
                                                      committed_offset 推进
                                                          │
Worker ──(Response)──▶ 所属 Sub Reactor 写队列 ──▶ EPOLLOUT ──▶ Client
```

接口约定（`include/mq/` 头文件为准）：

- 存储层：`StorageEngine`（§4.2 接口：Append/Fetch/CreateTopic/...）。
- 路由层：`QueueManager` 使用 `shared_mutex` 保护 Topic 元数据；创建为低频独占写，路由与列表为共享读。指定分区必须在范围内，自动路由使用稳定的 FNV-1a key hash。
- 网络层：`EventLoop` / `TcpServer` / `TcpConnection` / `ProtocolCodec`。
- 并发层：`ThreadPool` / `MPMCQueue` / `SpscRingBuffer` / `MemoryPool`。
- 分层规则：上层可调用下层，下层禁止回调上层；跨层只传数据不传对象所有权（除 shared_ptr 生命周期管理）。

## 6. 实现顺序建议（依赖拓扑）

1. `core/buffer`（MPMC/SPSC）+ `core/thread_pool`（无下层依赖）
2. `core/storage`（依赖 buffer，自包含可单测）
3. `protocol` 编解码（无依赖，可单测）
4. `network`（依赖 protocol、buffer）
5. `server`（组装 network + core，入口 main）
6. `client`（仅依赖 protocol，可独立测试）
7. `bench` / `tools`（验证吞吐与运维）
