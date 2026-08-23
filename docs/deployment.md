# mq-project 部署运维文档

> 版本: v0.1 · 更新: 2026-08-19 · 状态: P1 开发前规划

## 1. 部署拓扑

```
                    ┌──────────────┐
   Producer ──TCP──▶│              │
                    │   Broker A   │
   Consumer ◀──TCP──│  (Leader)    │
                    │   :9092      │
                    └──────┬───────┘
                           │ 复制 (P2)
                    ┌──────▼───────┐
                    │   Broker B   │
                    │  (Follower)  │
                    │   :9092      │
                    └──────────────┘
```

- 单机部署：1 × Broker（P1 阶段）。
- 高可用部署：≥ 2 节点，Leader/Follower 角色（P2 阶段，见 `docs/architecture.md` §6）。

## 2. 构建与安装

```bash
# 依赖：CMake ≥ 3.16、GCC ≥ 9 / Clang ≥ 12、spdlog、gtest（仅测试）
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
# 产物：build/src/mq_broker
# 安装（可选）
cmake --install build --prefix /opt/mq
```

## 3. 配置项

| 配置 | 默认 | 说明 |
|------|------|------|
| `listen.address` | `0.0.0.0` | 监听地址 |
| `listen.port` | `9092` | 监听端口 |
| `data_dir` | `/var/lib/mq` | 数据目录（禁止共用 NFS） |
| `threads.reactor` | CPU 核数（自动模式上限 32） | Sub Reactor 线程数 |
| `threads.worker` | CPU 核数 × 2 | Worker 线程数 |
| `storage.segment_size` | `64MiB` | 段文件大小 |
| `storage.index_interval` | `1000` | 每写入多少条消息追加一条稀疏索引 |
| `storage.fsync_policy` | `per_batch` | `per-message` / `per-batch` / `interval` |
| `storage.fsync_interval_ms` | `5` | 定时 fsync 间隔 |
| `storage.retention_ms` | `7d` | 按消息时间清理段 |
| `storage.retention_bytes` | `1GiB` | 按总段大小清理段 |
| `storage.cleaner_interval_ms` | `1000` | 后台清理扫描周期 |
| `storage.retention_ms` | `7d` | 消息保留时长（TTL） |
| `network.max_connections` | `50000` | 最大连接数 |
| `network.buffer_limit` | `8MB` | 单连接读写缓冲上限 |
| `session.heartbeat_timeout_ms` | `30000` | 消费者会话心跳超时 |
| `producer.ack_default` | `1` | 默认 ack 档位 |
| `memory.write_buffer_limit` | `512MB` | 写缓冲总量上限 |
| `memory.pool_size` | `2GB` | 内存池上限 |

## 4. 启动与停止

```bash
# 前台启动
/opt/mq/bin/mq_broker --config /etc/mq/broker.toml
# systemd
systemctl start mq-broker
systemctl stop mq-broker     # SIGTERM：flush + 安全退出
```

Broker 当前支持 INI 配置文件，示例见 `conf/broker.conf`。启动入口会加载监听地址/端口、数据目录、Sub Reactor 数量、段大小和保留时长；收到 SIGTERM/SIGINT 后停止接收连接、停止 Reactor、刷新 WAL 后退出。

消费者位点保存在 `data/metadata/consumer_offsets.meta`，采用临时文件加原子替换，与 Topic 元数据快照一致。P2 的副本协调状态目前为进程内状态，重启后需由节点心跳重新建立，尚未作为持久化元数据发布。

P2 复制配置示例：

```ini
node_id = node-leader
replica_role = leader
replica_peers = node-follower:127.0.0.1:9093
```

Follower 将 `replica_role` 设为 `follower`，并把 Leader 放入 `replica_peers`。Follower 每 250ms 拉取各分区增量并按连续 offset 写入本地 WAL；Leader 周期发送心跳。`ack=all` 需要本地写入加至少一个可达副本确认，否则返回 `STORAGE_ERROR`。

- 优雅停机：接收 SIGTERM → 停止 accept → 停止 Worker → flush 写缓冲 + fsync → 关闭段文件 → 退出。
- 强制终止（kill -9）安全：恢复时截断未写完记录（见 `docs/design-details.md` §1.7）。

## 5. 运维操作

### 5.1 常用命令（`tools/` 规划）
```bash
tools/mq-topic create <topic> --partitions 8
tools/mq-topic list
tools/mq-topic delete <topic>
tools/mq-check --data-dir /var/lib/mq          # 数据目录完整性检查（CRC/索引）
tools/mq-stats --data-dir /var/lib/mq          # 段文件与积压统计
```

### 5.2 容量规划
| 参数 | 参考公式 |
|------|----------|
| 磁盘 | 消息峰值速率 × 保留时长 × 副本数 × (1 + 20% 索引/开销) |
| 内存 | 写缓冲(512MB) + 连接数 × 每连接缓冲(上限 8MB) + 池(2GB) |
| 文件描述符 | max_connections + 数据文件数 + 预留 1024，`ulimit -n` 须 ≥ 此值 |
| 每分区 | 单分区写吞吐受磁盘顺序写限制（约 500MB/s，SSD） |

### 5.3 监控
- 指标暴露：`/metrics`（文本）或日志周期输出（见 PRD §5.3 指标清单）。
- 关注项与告警阈值（建议）：
  | 指标 | 阈值 | 级别 |
  |------|------|------|
  | 生产 TPS 下降 | < 基线 50% 持续 5min | warning |
  | 生产 p99 | > 基线 2× 持续 5min | warning |
  | 队列积压 | > 100MB 持续 10min | warning |
  | 写缓冲水位 | > 80% | warning |
  | 数据目录剩余 | < 20% | critical |
  | 连接数 | > 80% × max_connections | warning |

### 5.4 日志
- 日志级别与输出见 `AGENTS.md` §3.5；建议生产配置 `info`。
- 日志落盘路径可配置，禁止输出消息体明文。
- 当前 `mq::core::Logger` 默认写 stderr，可配置单文件滚动；单条日志限制为 4KiB，滚动时保留一个 `.1` 备份文件。

## 6. 备份与灾难恢复（DR）

| 项 | 策略 |
|----|------|
| 在线备份 | 复制数据目录（先 `flush`），或在低峰期对每个分区执行 `tools/mq-stats` 一致性快照 |
| 恢复 | 用备份目录替换数据目录 → 启动即自动 Recovery（截断半条记录、重建索引） |
| RPO | 在线备份 = fsync 窗口（默认 ≤5ms）；每日全量备份 = 24h 级 |
| RTO | 单机恢复 ≈ 数据量 / 扫描速率 + 索引重建时间，目标 < 5min（<100GB 数据） |
| 多副本 | P2 主从复制可降至 RPO≈0、RTO≈切换时间 |

### 6.1 恢复演练清单
1. 停服务；2. 备份现场数据目录；3. 从备份恢复；4. 启动并观察 Recovery 日志；5. `tools/mq-check` 校验；6. 生产/消费冒烟测试。

## 7. 上线检查清单

- [ ] 配置项与容量规划核对（§3、§5.2）
- [ ] `ulimit -n` / 文件描述符充足
- [ ] 数据目录所在磁盘性能与剩余空间达标
- [ ] 防火墙放行监听端口；无 TLS（明文）场景确认网络可信
- [ ] 监控与告警接入（§5.3）
- [ ] 压测达标（`docs/performance-baseline.md`）

### 7.1 P2 高可用检查

- [ ] 副本节点使用唯一 `node_id`，并配置至少 3 个节点以保证分区下仍可形成多数派。
- [ ] `ack=all` 仅在多数派可达时使用；少于多数派时客户端应处理 `STORAGE_ERROR`/`NOT_LEADER` 并切换 endpoint。
- [ ] 故障演练需验证旧 Leader 分区后拒绝写入、候选节点获得多数票后接管，以及旧节点恢复后以 Follower 身份重新同步。
