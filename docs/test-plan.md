# mq-project 测试方案

> 版本: v0.2 · 更新: 2026-08-27

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

## 3. 单元测试用例

### 3.1 存储引擎 `storage_engine_test`
- [x] 追加/读取往返一致（含跨段回卷）
- [x] CRC 损坏检测
- [x] 崩溃恢复（截断半条记录）
- [x] 稀疏索引定位正确性
- [x] 段滚动、按大小保留清理与跨段读取

### 3.2 编解码 `codec_test`
- [x] 请求/响应帧编解码往返
- [x] 请求半包与粘包（分片读取）解析
- [x] 字段越界/畸形输入拒绝与未知版本返回 `VERSION_MISMATCH`
- [x] 随机受限输入、超长长度字段和随机分片回归（`mq_protocol_fuzz_tests`）
- [x] 提供并实际运行 Clang libFuzzer 覆盖引导式入口（`MQ_BUILD_FUZZERS=ON`，2000 次变异）

Clang Fuzzer 构建要求安装与编译器主版本匹配的 compiler-rt 开发包。例如 Ubuntu 24.04 使用 Clang 18：

```bash
sudo apt-get update
sudo apt-get install -y clang-18 llvm-18 libclang-rt-18-dev
rm -rf build-fuzzer
CC=clang-18 CXX=clang++-18 cmake -S . -B build-fuzzer \
  -DMQ_BUILD_TESTS=ON -DMQ_BUILD_TOOLS=OFF -DMQ_BUILD_FUZZERS=ON
cmake --build build-fuzzer --parallel
ctest --test-dir build-fuzzer --output-on-failure
```

配置阶段会用当前编译器实际执行 `-fsanitize=fuzzer,address,undefined` 的编译和链接探测，不依赖发行版具体的 runtime 文件名或目录布局。如果使用 Clang 21，必须安装 `libclang-rt-21-dev`；若发行版仓库没有该包，应改用仓库提供的 Clang 18 工具链，不能混用 Clang 21 编译器和 Clang 18 的运行库。CI 还会打印 resource dir 和 runtime 查询结果，便于定位工具链安装问题。

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
- [x] Topic 删除后清理 WAL，重建同名 Topic 从 offset 0 开始
- [x] 幂等生产并发重试只写入一次

### 3.8 客户端 SDK `client_test`
- [x] Producer/Consumer 回环连接、批量生产和消费提交
- [x] Producer metadata sequence 与 Broker 幂等响应
- [x] ack=0 / ack=1 / ack=all 语义

### 3.9 安全传输与鉴权
- [x] 配置客户端 Token 后拒绝缺失/错误 Token，并接受正确 Token
- [x] 已认证客户端按 admin/produce/consume 权限和 Topic 白名单执行授权
- [x] OpenSSL TLS 1.2+ 握手、证书校验和加密请求回环（`mq_tls_integration_tests`，仅 TLS 构建）

### 3.10 性能与 sanitizer
- [x] Linux 标准矩阵生产场景脚本与结果完整性/回归门禁（`tools/check_bench_matrix.sh`）
- [x] 固定硬件正式基线：VM16 Linux Release 完成 batch=1/100/1000、connections=1/10、256 B/4096 B、3 分区生产矩阵及 1,000,000 条消费基线
- [x] Linux ASan/UBSan 构建并运行全量 CTest
- [x] WSL2 Linux Debug 构建并运行全量 CTest

## 4. 集成测试用例（P1 起）

- [x] 进程内 Broker：创建 Topic → 生产 → 拉取消息
- [x] 进程内 Broker：创建 Topic → 重启 → Topic 元数据恢复
- [x] 单 Broker：生产 N 条 → 消费 N 条顺序一致
- [x] 多消费者组位点独立提交
- [x] 重启后数据恢复（WAL replay）
- [x] ack=all / ack=1 / ack=0 语义

### 4.1 TCP 回环集成 `tcp_integration_test`
- [x] 1000 个并发连接通过 Reactor 服务正确收发协议帧（Linux epoll / Windows select）

### 4.2 P2 复制与故障切换
- [x] ReplicationClient Fetch/Append/Heartbeat/Vote TCP 通信
- [x] 任期递增、低任期请求拒绝和多数派选举
- [x] 多数派 commit index 与 ack=all
- [x] 多 endpoint 客户端收到 NOT_LEADER 后切换
- [x] 复制请求认证、成员校验和日志连续性校验

### 4.3 P3 生产保护与指标
- [x] `METRICS` 返回 Prometheus 文本指标
- [x] 生产请求按秒限流，超限返回 `RATE_LIMITED`
- [x] 批量生产按请求计数，复制内部请求不受生产限流影响
- [x] Topic 每秒生产字节配额，单条与批量超限拒绝且批量不部分写入
- [x] 配额指标暴露当前配置与窗口使用量
- [x] `mq_admin topics`/`metrics` TCP 管理查询与帮助命令

## 5. 压测项（P1 起，`bench`）

- 生产/消费吞吐（batch 1/100/1000）
- p50/p99/p999 延迟
- 5w 连接并发稳定性
- 段滚动与过期清理下的稳定性

## 6. 测试数据隔离

- 单测使用系统临时目录，测后清理。
- 集成测试使用独立随机端口与数据目录，避免端口/目录冲突。

当前已落地的核心测试覆盖：协议请求帧往返与截断输入拒绝；WAL 追加/读取、重启恢复和损坏尾部截断。
