# mq-project 测试方案

> 版本: v0.1 · 更新: 2026-08-19

## 1. 测试层次

| 层次 | 位置 | 框架 | 覆盖范围 |
|------|------|------|----------|
| 单元测试 | `test/unit` | gtest | 存储、编解码、环形队列、线程池 |
| 集成测试 | `test/integration` | gtest + 进程 | Broker 全链路（生产→消费） |
| 压测 | `bench` | 自定义 | 吞吐、延迟、连接数 |

## 2. 构建与运行

```bash
cmake -S . -B build -DMQ_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## 3. 单元测试用例（骨架期规划）

### 3.1 存储引擎 `storage_engine_test`
- [x] 追加/读取往返一致（含跨段回卷）
- [ ] CRC 损坏检测
- [ ] 崩溃恢复（截断半条记录）
- [x] 稀疏索引定位正确性
- [x] 段滚动、按大小保留清理与跨段读取

### 3.7 客户端 SDK `client_test`
- [x] Producer/Consumer 回环连接、批量生产和消费提交
- [x] Producer metadata sequence 与 Broker 幂等响应
- [x] ack=0 / ack=1 / ack=all 语义

### 5.1 性能与 sanitizer
- [ ] Linux 标准矩阵：batch=1/100/1000、connections=1/10、256 B/4096 B、3 分区
- [ ] Linux ASan/UBSan 构建并运行全量 CTest

### 3.2 编解码 `codec_test`
- [ ] 请求/响应帧编解码往返
- [x] 请求半包与粘包（分片读取）解析
- [ ] 字段越界/畸形输入拒绝

### 3.3 并发原语 `buffer_test`
- [x] MPMC 环形队列满/空行为
- [x] MPMC 多生产者多消费者一致性
- [x] 线程池任务执行与关闭时排空
- [ ] SPSC 环形队列满/空行为
- [ ] 线程池任务异常的调用方可观测策略

### 3.4 EventLoop `event_loop_test`
- [x] 跨线程任务在 owner 线程执行
- [x] 停止时排空已接收任务并拒绝后续投递
- [x] idle 轮询回调在 owner 线程执行

### 3.5 网络缓冲与连接 `network_buffer_connection_test`
- [x] 内存池的 owner-thread 限制与容量上限
- [x] 固定读缓冲压缩与背压边界
- [x] Worker 回投写缓冲并由 owner loop 消费

### 3.6 日志 `logger_test`
- [x] 日志级别过滤与滚动文件写入

### 3.7 Broker 元数据恢复 `broker_test`
- [x] 创建 3 个不同分区数的 Topic 后重新构造 Broker，验证 `listTopics` 的名称、排序与分区数完全恢复
- [x] 验证创建 Topic 后生成 `data/metadata/topics.meta` 快照

### 4.1 TCP 回环集成 `tcp_integration_test`
- [x] 1000 个并发连接通过 Reactor 服务正确收发协议帧（Linux epoll / Windows select）

## 4. 集成测试用例（P1 起）

- [x] 进程内 Broker：创建 Topic → 生产 → 拉取消息
- [x] 进程内 Broker：创建 Topic → 重启 → Topic 元数据恢复
- [ ] 单 Broker：生产 N 条 → 消费 N 条顺序一致
- [ ] 多消费者组位点独立提交
- [ ] 重启后数据恢复（WAL replay）
- [ ] ack=all / ack=1 / ack=0 语义

## 5. 压测项（P1 起，`bench`）

- 生产/消费吞吐（batch 1/100/1000）
- p50/p99/p999 延迟
- 5w 连接并发稳定性
- 段滚动与过期清理下的稳定性

## 6. 测试数据隔离

- 单测使用系统临时目录，测后清理。
- 集成测试使用独立随机端口与数据目录，避免端口/目录冲突。

当前已落地的核心测试覆盖：协议请求帧往返与截断输入拒绝；WAL 追加/读取、重启恢复和损坏尾部截断。
