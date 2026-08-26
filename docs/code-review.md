# 代码审查报告

> 审查日期: 2026-08-26
> 审查范围: 全部 49 个源文件（~220KB），覆盖存储、网络、服务端、协议、客户端、测试与文档
> 综合评分: **6.5 / 10**

项目已完成 P1 基础链路 + P2 复制能力，整体架构清晰、分层合理，WAL/CRC/Reactor 模型实现完整。但存在若干影响正确性和可靠性的问题，以下按严重程度分类列出。

---

## 一、严重 Bug（必须立即修复）

### 1. WAL fsync 使用只读 fd — 落盘保护形同虚设

**位置**: `src/core/storage/storage_engine.cc:81-83`

```cpp
const int fd = open(path.c_str(), O_RDONLY);
if (fd < 0) return false;
const int result = fsync(fd);
```

**问题**: `O_RDONLY` 打开的文件描述符调用 `fsync()` 在 Linux 上返回 `EBADF`（失败）或静默不执行实际刷盘。所有 `SyncPath` 调用（ack=1、ack=all、Flush）均依赖此函数，意味着 WAL 的 fsync 保护完全失效，掉电时已确认写入的数据可能丢失。

**修复**: 将 `open` 的 flag 改为 `O_RDWR`，或直接使用数据文件已打开的 fd 进行 fsync。

---

### 2. 版本协商未实现（文档与代码不一致）

**位置**: `src/protocol/protocol_codec.cc:38-41`、`docs/api-spec.md:94-98`

api-spec.md 明确要求：
- 客户端握手时携带 `min_version / max_version`
- Broker 返回协商结果与自身版本
- 版本不兼容返回 `VERSION_MISMATCH` + `current_version`

但 `ProtocolCodec::DecodeRequest/DecodeResponse` 中只做了硬拒绝：

```cpp
if (version != kCurrentVersion) {
  if (error != nullptr) *error = "unsupported protocol version";
  return false;
}
```

**影响**: 违反 AGENTS.md §3.7 硬性约束，旧版客户端无法识别版本不兼容的原因，也无法优雅降级。

---

### 3. 幂等缓存无界增长

**位置**: `src/server/broker.cc:478-518`

`idempotency_cache_` 是 `std::map<std::string, protocol::Response>`，只有插入没有淘汰。长时间运行的 Broker 内存会无限膨胀。

**修复**: 添加 TTL 淘汰（如保留最近 5 分钟的条目）或 LRU 上限。

---

### 4. Topic 删除不清理 WAL 数据

**位置**: `src/server/broker.cc:393-407`

`HandleDeleteTopic` 只删除 metadata，WAL 目录（`data/queues/<topic_hex>/`）和段文件永远不会被清理。

**修复**: 删除 metadata 后，异步或同步删除对应的 WAL 目录。

---

## 二、复制 / 高可用缺陷

### 5. 选举不检查日志完整性

**位置**: `src/server/replication.cc:36-43`、`src/server/broker.cc:168-182`

`RequestVote` 只比较 term，不检查 candidate 的 `lastLogIndex / lastLogTerm`。这是 Raft 的核心安全属性——日志落后的节点不应当选 Leader。

**影响**: 日志落后的节点可能当选 Leader 并覆盖更新的日志，导致数据丢失。

---

### 6. 无日志截断 / 快照机制

**位置**: 全局缺失

Follower 长期落后时，Leader 只能逐条追赶。没有 snapshot 能力，对大规模数据场景不可行。

---

### 7. 复制使用短连接 — 性能极差

**位置**: `src/server/broker.cc:144, 173, 185, 526-527`

`ReplicationLoop` 每次迭代对每个 peer 新建 TCP 连接（`ReplicationClient client(host, port)`）。高频心跳 + 数据复制场景下连接开销巨大。

**修复**: 改为长连接 + 连接池复用。

---

### 8. 选举超时固定，无随机化

**位置**: `src/server/broker.cc:168`

```cpp
if (!leader_seen && std::chrono::steady_clock::now() - last_election >= std::chrono::seconds(1))
```

固定 1 秒间隔，多节点同时选举时 split-brain 风险高。Raft 要求随机化选举超时（如 150-300ms）。

---

### 9. `AdvanceCommit` 在 term 切换时不重置 commitIndex

**位置**: `src/server/replication.cc:50-52`

```cpp
bool ReplicationCoordinator::AdvanceCommit(std::uint64_t offset) {
  ...
  if (role_ != ReplicaRole::kLeader || offset > local_offset_) return false;
  ...
  commit_index_ = std::max(commit_index_, offset);
  last_applied_ = commit_index_;
  return true;
}
```

Leader 切换后 `commit_index_` 可能指向旧任期的未验证数据。Raft 要求新 Leader 只提交当前任期的日志条目（§5.4.2）。

---

### 10. `CanServeWrites` 在 Leader 获得新 peer 后可能误判

**位置**: `src/server/replication.cc:16-22`

```cpp
bool ReplicationCoordinator::CanServeWrites() const {
  ...
  if (replicas_.empty()) return true;
  std::size_t alive = 1;
  for (const auto& [id, replica] : replicas_)
    if (Healthy(replica, now)) ++alive;
  return alive >= Majority();
}
```

新注册的 peer（`last_heartbeat` 为 epoch）初始状态不健康，导致 Leader 在只有一个新 peer 时可能错误地停止写入。

---

## 三、网络层问题

### 11. 无写背压机制

**位置**: `src/network/tcp_connection.cc:96-101`

```cpp
bool TcpConnection::AppendWrite(std::string_view bytes) {
  if (bytes.size() > write_capacity_ - queued_write_bytes_) return false;
  ...
}
```

写队列满时静默返回 false，数据丢弃，对端不知道消息丢失。应该关闭 EPOLLIN（背压）或返回错误给调用方。

---

### 12. Worker 队列满时直接断开连接

**位置**: `src/network/tcp_server.cc:141`

```cpp
if (!workers.Submit([...])) { CloseClient(sub, fd); return; }
```

`workers.Submit` 失败只意味着 Worker 暂时繁忙，连接本身没有问题。应该排队等待或返回 `RESOURCE_EXHAUSTED`，而不是断连。

---

### 13. 读缓冲仅 64KB

**位置**: `src/network/tcp_server.cc:32`

```cpp
static constexpr std::size_t kConnectionReadBufferBytes = 64 * 1024;
```

高吞吐场景下可能频繁触发部分读。建议增大到 256KB-1MB。

---

## 四、代码质量

### 14. Put/Get 函数在 6+ 个文件中重复定义

以下文件各自独立实现了相同的 `Put16/32/64` 和 `Get16/32/64`：

| 文件 |
|------|
| `src/core/storage/storage_engine.cc` |
| `src/server/broker.cc` |
| `src/protocol/protocol_codec.cc` |
| `src/client/mq_client.cc` |
| `src/server/replication_client.cc` |
| `test/unit/broker_test.cc` |

**修复**: 提取到 `include/mq/protocol/codec_utils.h` 共享。

---

### 15. 客户端 producer_id 用时间戳 — 碰撞风险

**位置**: `src/client/mq_client.cc:55`

```cpp
producer_id = static_cast<std::uint64_t>(
    std::chrono::steady_clock::now().time_since_epoch().count());
```

两个同时启动的客户端实例会得到相同 ID，导致幂等键冲突（Broker 按 `producer_id:sequence` 去重）。

**修复**: 使用 `random_device` 或 UUID 生成唯一 ID。

---

### 16. `MqConsumer` 继承 `MqProducer`

**位置**: `src/client/mq_client.cc:154`

```cpp
struct MqConsumer::Impl : MqProducer::Impl { ... };
```

语义上消费和生产是不同的概念，继承只为复用 socket 代码。违反单一职责原则，且 `MqConsumer` 暴露了 `produce`、`createTopic` 等无关方法。

---

### 17. 内存池线程亲和性断言缺失

**位置**: `include/mq/core/memory_pool.h:19`

`IsOwnerThread()` 方法存在但从未被调用。跨线程使用线性内存池会导致释放竞态，但没有任何断言保护。

**修复**: 在 `Allocate` 开头添加 `assert(IsOwnerThread())` 或 debug 模式下检查。

---

### 18. 限流器固定窗口精度问题

**位置**: `src/server/broker.cc:95-106`

```cpp
if (now - produce_window_start_ >= std::chrono::seconds(1)) {
  produce_window_start_ = now;
  produce_window_count_ = 0;
}
```

固定窗口在边界处允许 2x 突发流量（窗口开头和结尾各放过配额量）。生产环境应使用滑动窗口或令牌桶。

---

## 五、测试与工程

### 19. 测试覆盖薄弱

| 指标 | 现状 |
|------|------|
| 单元测试文件 | 8 个 |
| 集成测试 | 仅 TCP 集成测试 1 个 |
| 复制场景覆盖 | 仅基础 append + offset gap 测试 |
| Fuzz 测试 | 无（协议解析器应优先 fuzz） |
| 并发 Stress Test | 无（Sanitizer 构建存在但无并发压测） |
| 测试框架 | 原生 `assert()`，失败时无用例定位 |

---

### 20. 热路径日志风险

**位置**: `src/server/broker.cc:202`

`Handle()` 中的 `kError` 日志在高错误率时成为瓶颈（Logger 内部有 mutex）。热路径应避免日志或使用无锁日志。

---

## 六、缺失功能清单（P3 方向）

| 功能 | 优先级 | 说明 |
|------|--------|------|
| TLS / 认证授权 | 高 | 当前明文 TCP，不适合生产环境 |
| Consumer Group 分区分配 / Rebalance | 高 | 当前需手动指定分区 |
| 消息 TTL / 死信队列 | 中 | 无消息过期和失败重试机制 |
| 消息压缩（gzip/snappy/zstd） | 中 | flags 预留但未实现 |
| Connection Pool | 中 | 客户端和复制均用短连接 |
| Benchmark 正式化 | 中 | 仅 1 个 bench 文件 |
| 快照 / 日志截断 | 高 | 复制长期追赶必须 |
| 滑动窗口限流 | 低 | 固定窗口精度不足 |
| 配置热加载 | 低 | 当前修改配置需重启 Broker |

---

## 七、建议修复优先级

| 优先级 | 项目 |
|--------|------|
| **P0 立即** | SyncPath fd 模式、幂等缓存淘汰、Topic 删除清理 WAL |
| **P0 尽快** | 版本协商实现、选举日志完整性检查 |
| **P1 下一迭代** | 复制长连接、写背压、内存池断言、代码去重 |
| **P2 规划中** | TLS、Consumer Rebalance、fuzz 测试、滑动窗口限流 |
| **P3 远期** | 快照机制、消息压缩、配置热加载 |

---

## 八、未提交改动评估

`git diff` 显示 27 个文件 +87/-28 行未提交。改动内容为：
- 添加 BOM 标记（`﻿`）
- 添加中文注释说明设计意图

改动本身安全，建议尽快 commit。
