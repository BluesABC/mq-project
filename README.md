# mq-project

`mq-project` 是一个使用 C++17 编写的高性能消息队列服务，包含 Broker、客户端 SDK 和管理工具。项目以较少的第三方依赖为目标，围绕 TCP Reactor、WAL 持久化、分区队列和并发原语构建。

项目适合用于微服务解耦、异步任务、日志/事件管道等场景，也适合作为学习消息队列内部实现、网络编程、持久化和并发设计的工程样例。

## 当前状态

当前代码已经完成 P1 基础链路、P2 复制能力，并完成 P3 核心生产化与安全验证：

- Topic 创建、删除、查询和元数据持久化
- 按 Topic/Partition 生产与消费，支持批量生产
- Consumer Group 位点提交与恢复
- WAL 日志追加、CRC 校验、崩溃尾部恢复和日志段滚动
- TCP 长连接、半包/粘包处理、固定连接线程归属和 Reactor 网络模型
- `ack=0`、`ack=1`、`ack=all` 生产确认模式
- Leader/Follower 增量复制、复制心跳、quorum ack 和客户端多端点重连
- Prometheus 文本指标、生产限流和 Topic 字节配额
- `mq_admin` 管理工具，可查询 Topic 和指标
- 可选 OpenSSL TLS 1.2+、客户端 Token 认证及 admin/produce/consume ACL
- 结构化协议回归测试、Clang libFuzzer 入口和 GitHub Actions CI 门禁
- 单元测试、TCP 集成测试、复制测试和基准测试

复制协调器目前还不是完整的 Raft/ZAB 实现，生产部署前应结合 `docs/deployment.md` 和故障恢复要求进行验证。TLS 默认关闭以保持兼容；生产部署必须启用 TLS 或置于可信 TLS 代理之后，并配置客户端 Token 与 ACL。

## 架构概览

```text
Producer / Consumer
        |
        | TCP binary protocol
        v
Main Reactor -> Sub Reactors -> Worker Pool
                                      |
                                      v
                              Broker / Queue Manager
                                      |
                                      v
                              Storage Engine (WAL)
                                      |
                                      v
                              Follower replication
```

主要模块职责如下：

| 模块 | 目录 | 说明 |
| --- | --- | --- |
| 协议层 | `src/protocol` | 请求/响应帧、命令和状态码编解码 |
| 网络层 | `src/network` | EventLoop、TCP Server、连接和网络缓冲 |
| 核心层 | `src/core` | Topic、队列路由、WAL、线程池、内存池和位点存储 |
| 服务端 | `src/server` | Broker、复制协调和 Broker 主入口 |
| 客户端 | `src/client` | `MqProducer` 和 `MqConsumer` SDK |
| 工具 | `tools` | `mq_admin` 和基准矩阵检查脚本 |

同一连接的事件固定由一个 Reactor 线程处理；业务任务投递到 Worker 线程池；持久化写入统一经过 Storage Engine。协议字段和状态码以 [`docs/api-spec.md`](docs/api-spec.md) 为准。

## 环境要求

- CMake 3.16 或更高版本
- C++17 编译器
  - Linux：GCC 9+ 或 Clang 12+
  - Windows：支持 C++17 的 Visual Studio/MSVC
- Linux 是主要性能验证平台；Windows 提供可编译和测试支持
- 运行 Linux 测试脚本需要 `bash`
- 启用 TLS 需要 OpenSSL 开发包；运行格式门禁需要 clang-format 18+

项目尽量减少依赖。当前构建使用 C++ 标准库和系统线程/Socket 能力；测试目标使用仓库内的断言式测试代码，不要求额外安装 gtest。

## 快速开始

### Linux / macOS

```bash
cmake -S . -B build -DMQ_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/mq_broker --config conf/broker.conf
```

### Windows PowerShell

```powershell
cmake -S . -B build -DMQ_BUILD_TESTS=ON
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
.\build\Debug\mq_broker.exe --config conf\broker.conf
```

Broker 默认监听 `127.0.0.1:9092`，数据写入 `data/`，日志写入 `data/broker.log`。首次运行会创建所需的数据目录。

停止 Broker 时发送 `SIGINT`/`SIGTERM`，或在 Windows 控制台按 `Ctrl+C`。Broker 会先停止网络服务并 flush 已缓冲数据。

## 管理工具

启用工具构建后，可以查询 Topic 和 Prometheus 指标：

```bash
cmake -S . -B build -DMQ_BUILD_TOOLS=ON
cmake --build build -j

./build/mq_admin topics --host 127.0.0.1 --port 9092
./build/mq_admin metrics --host 127.0.0.1 --port 9092
```

Windows 下将可执行文件路径替换为 `build\Debug\mq_admin.exe`。完整帮助：

```text
mq_admin metrics|topics [--host HOST] [--port PORT] [--timeout-ms MS]
             [--auth-token TOKEN]
```

## 客户端 SDK

公共头文件位于 `include/mq/client/mq_client.h`。SDK 提供：

- Producer：创建 Topic、单条/批量生产、flush、Topic 查询和指标查询
- Consumer：订阅 Topic 或指定分区、poll、commit
- `AckMode::kZero`、`AckMode::kOne`、`AckMode::kAll`
- 多 Broker endpoint 连接、超时配置和断线重连
- `setAuthToken` 客户端身份认证，`setTlsOptions` TLS 证书校验配置

最小生产流程示例：

```cpp
#include "mq/client/mq_client.h"

int main() {
  mq::client::MqProducer producer;
  if (!producer.connect("127.0.0.1", 9092)) return 1;
  if (!producer.createTopic("events", 3)) return 1;

  mq::client::ProduceResult result;
  if (!producer.produce("events", "order-42", "created",
                        mq::client::AckMode::kOne, &result)) {
    return 1;
  }
  producer.close();
}
```

消费端通过 `MqConsumer::subscribe()` 订阅，再使用 `poll()` 获取消息并通过 `commit()` 提交位点。消息大小、Topic 名称、请求长度等输入均受协议限制，具体限制见 [`docs/api-spec.md`](docs/api-spec.md)。

## 配置

默认配置文件为 [`conf/broker.conf`](conf/broker.conf)，常用配置如下：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `bind_address` | `127.0.0.1` | 监听地址 |
| `bind_port` | `9092` | TCP 监听端口 |
| `data_dir` | `data` | WAL 和元数据目录 |
| `sub_reactor_threads` | `0` | `0` 表示按机器资源决定 |
| `segment_size` | `64Mi` | 日志段大小 |
| `retention_hours` | `168` | 数据保留时间 |
| `produce_rate_limit` | `0` | 每秒生产请求数，`0` 表示关闭 |
| `topic_produce_quota_bytes` | `0` | 每 Topic 每秒 key/value 字节配额，`0` 表示关闭 |
| `client_auth_token` | 未设置 | 普通客户端 Token；配置后启用身份认证 |
| `client_auth_permissions` | 未设置 | `admin`、`produce`、`consume` 权限列表 |
| `client_auth_produce_topics` | 未设置 | 生产 Topic 白名单 |
| `client_auth_consume_topics` | 未设置 | 消费 Topic 白名单 |
| `tls_enabled` | `false` | 是否启用 TLS |
| `tls_certificate_file` | 未设置 | Broker PEM 证书 |
| `tls_private_key_file` | 未设置 | Broker PEM 私钥 |
| `tls_ca_file` | 未设置 | mTLS 客户端 CA |
| `tls_require_client_certificate` | `false` | 是否要求客户端证书 |
| `node_id` | `node-local` | Broker 节点标识 |
| `replica_role` | `leader` | `leader` 或 `follower` |
| `replica_peers` | 未设置 | 复制节点，格式为 `node-id:host:port;...` |

配置项变更通常需要重启 Broker。复制部署和数据目录恢复流程见 [`docs/deployment.md`](docs/deployment.md)。

## 测试与基准

运行完整测试：

```bash
cmake -S . -B build -DMQ_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

启用基准程序：

```bash
cmake -S . -B build -DMQ_BUILD_BENCH=ON
cmake --build build -j
./build/mq_bench --help
```

Sanitizer 构建（GCC/Clang）：

```bash
cmake -S . -B build-sanitize \
  -DMQ_BUILD_TESTS=ON -DMQ_ENABLE_SANITIZERS=ON
cmake --build build-sanitize -j
ctest --test-dir build-sanitize --output-on-failure
```

TLS 构建与测试：

```bash
sudo apt-get install -y libssl-dev openssl
cmake -S . -B build-tls -DMQ_BUILD_TESTS=ON -DMQ_ENABLE_TLS=ON
cmake --build build-tls -j
ctest --test-dir build-tls --output-on-failure
```

Clang 引导式 Fuzz：

```bash
cmake -S . -B build-fuzzer -DMQ_BUILD_TESTS=ON -DMQ_BUILD_FUZZERS=ON \
  -DCMAKE_CXX_COMPILER=clang++
cmake --build build-fuzzer -j
ctest --test-dir build-fuzzer --output-on-failure
```

提交前还应执行：

```bash
clang-format --dry-run --Werror $(git ls-files '*.h' '*.cpp')
```

## 目录导航

```text
include/mq/       公共头文件
src/              Broker、SDK 和各基础模块实现
test/unit/        单元测试
test/integration/ 集成测试
bench/            吞吐和延迟基准
conf/             Broker 配置示例
docs/             架构、协议、部署、测试和性能文档
tools/            管理工具、基准检查和发布清单
```

建议阅读顺序：

1. [`docs/architecture.md`](docs/architecture.md)：总体架构和线程模型
2. [`docs/api-spec.md`](docs/api-spec.md)：协议、命令和状态码
3. [`docs/design-details.md`](docs/design-details.md)：存储、并发和网络细节
4. [`docs/deployment.md`](docs/deployment.md)：部署、复制和恢复
5. [`docs/test-plan.md`](docs/test-plan.md)：测试范围和验收方法

## 开发约束

请先阅读 [`AGENTS.md`](AGENTS.md)。重要约束包括：

- 使用 C++17，保持分层单向依赖
- 协议或错误码变更必须先更新 `docs/api-spec.md`
- 所有持久化写入必须经过 `StorageEngine`
- 跨线程共享数据必须明确内存序和生命周期
- 改动并发、存储格式或协议时同步更新对应文档和测试
- 提交前确保构建、CTest、格式检查和必要的 sanitizer 验证通过

## 许可证与项目说明

仓库当前未提供独立的 LICENSE 文件。对外发布或在其他项目中集成前，请先确认项目维护者的授权和许可证安排。
