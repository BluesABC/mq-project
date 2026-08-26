# AGENTS.md — 项目约束手册

> 本文件是进入本仓库（含 AI 助手与人工）都必须遵守的**项目级约束**，优先于个人习惯与编辑器默认设置。先读本文，再读 `docs/`。

## 1. 项目简介

自研高可用、高并发消息队列服务（Broker + 客户端 SDK），C++17 实现。
技术选型：**自研 epoll/io_uring Reactor 网络模型 + 磁盘 WAL 持久化存储 + Lock-Free 并发原语 + 最小第三方依赖（spdlog/gtest）**。

## 2. 仓库结构

```
mq-project/
├── PRD.md                  # 需求文档
├── AGENTS.md               # 本项目约束（本文件）
├── docs/
│   ├── architecture.md     # 架构总览
│   ├── design-details.md   # 模块内部实现细节（存储/网络/并发/内存）
│   ├── api-spec.md         # 协议/接口权威定义
│   ├── test-plan.md        # 测试方案
│   ├── deployment.md       # 部署运维与灾难恢复
│   └── performance-baseline.md  # 性能基线报告（bench 回归对照）
├── include/mq/             # 公开头文件（按模块子目录）
│   ├── core/               # 存储引擎、队列路由、线程池、内存池
│   ├── network/            # EventLoop/TcpServer/TcpConnection/Codec
│   ├── protocol/           # 协议枚举与编解码
│   ├── server/             # Broker/Handler
│   └── client/             # 客户端 SDK
├── src/                    # 实现（目录与 include/mq 一一对应）
│   ├── core/storage/       # WAL + mmap 段文件
│   ├── network/            # epoll/io_uring 后端
│   ├── protocol/           # 编解码实现
│   ├── server/             # Broker 装配、main
│   └── client/             # 客户端 SDK 实现
├── test/                   # unit/（gtest）+ integration/
├── bench/                  # 性能基准
└── tools/                  # 运维工具
```

## 3. 编码约束

### 3.1 语言与风格
- 语言标准：**C++17**（禁用 C++20 特性；异常可按模块开关，存储层建议开启）。
- 命名：类型 `PascalCase`；函数/变量 `snake_case`；常量 `kCamelCase`；枚举值全大写。
- 头文件：每个 `.h` 自包含可独立编译；一律 `#pragma once`；公开头放 `include/mq/<module>/`，内部头放 `src/<module>/`。
- 注释使用中文；只写"为什么"，不写"是什么"；**禁止冗余注释**（如 `// 定义变量 x`）。
- 格式化：以 `.clang-format` 为准，提交前必须格式化。

### 3.2 线程与并发正确性（硬性约束）
1. **网络层禁止阻塞**：Sub Reactor 线程内禁止文件 IO、锁竞争、日志落盘、业务计算；一律投递 Worker 池。
2. **连接固定线程**：同一连接的所有事件必须由同一 Reactor 线程处理，禁止跨线程操作连接对象。
3. **锁最小化**：热路径（消息读写）优先无锁结构（MPMC/SPSC 环形队列）；只有元数据变更才允许加锁。
4. **内存序**：无锁结构必须显式标注 `acquire/release/relaxed`，禁止依赖默认语义；跨线程共享数据必须配对内存屏障。
5. **禁止跨线程释放**：对象归还必须回到其创建线程（对象池），防止释放竞态。

### 3.3 架构分层约束
6. **协议冻结**：`src/protocol` 的字段布局是契约，与 `docs/api-spec.md` 必须一致；任何变更**先改 api-spec.md** 再改代码。
7. **存储访问唯一入口**：所有持久化写入必须走 `StorageEngine` 接口（WAL），**禁止**绕过 WAL 直接 `open/write` 数据文件。
8. **分层单向依赖**：上层可调下层，下层禁止回调上层；跨层传递只传数据，不传对象所有权（生命周期统一用 `shared_ptr` 托管）。
9. **依赖最小化**：新增第三方库必须先在 PRD 说明理由并经评审；禁止引入重型框架。
10. **不跨线程移动存储句柄**：存储引擎的分区写线程与 IO 线程分离，接口参数以值/引用传递，禁止隐式共享可变状态。

### 3.4 提交与质量约束
11. 提交前必须：`cmake --build build` 通过 + `ctest --test-dir build` 全绿。
12. 新功能必须有对应单测/集成测；性能敏感代码必须进 `bench/`。
13. 禁止提交密钥、本地绝对路径、临时文件；`.gitignore` 覆盖 `build/`、`*.log`、`*.idx` 等产物。
14. 改动存储格式/协议/并发结构时必须更新对应 `docs/` 文档，文档与代码同步是提交前置条件。

### 3.5 错误码与日志规范
15. **错误码唯一来源**：错误码必须取自 `docs/api-spec.md` 的 Status 枚举；新增错误码**先改 api-spec.md** 再改代码，禁止直接使用魔法数字。
16. **日志级别**：`trace` 调试细节 / `debug` 连接与请求流程 / `info` 启动、配置、周期性状态 / `warn` 可恢复异常（重试、背压触发）/ `error` 可恢复但影响局部的失败（单分区存储错误）/ `critical` 致命错误（启动失败、数据损坏）。热路径禁止打印日志。
17. **日志内容**：禁止打印消息体明文、密钥、完整凭据；允许打印 request_id、topic、partition、offset、长度等元信息。

### 3.6 内存与安全编码规范
18. **内存管理统一走内存池**：消息缓冲、连接对象、解码缓冲必须经 `MemoryPool`/对象池分配与归还（见 `docs/design-details.md` §4）；禁止热路径裸 `new`/`malloc`。
19. **内存上限可配置**：缓冲上限、池容量、段 mmap 窗口须可配置，超限触发背压或降级，禁止无界增长导致 OOM。
20. **输入校验**：所有网络输入（长度字段、offset、topic 名、flags）必须先校验再使用；长度字段必须防溢出/防越界（整数溢出检查）。
21. **安全基线**：不保存凭据与密钥；不执行来自网络路径的代码；错误信息不泄露内部路径/堆栈；`request_id` 与幂等表须防重放滥用（限流）。

### 3.7 协议版本协商
22. **版本协商**：握手时客户端携带 `min_version/max_version`，Broker 返回协商结果与自身版本；版本不兼容返回 `VERSION_MISMATCH` 并携带 `current_version`，客户端据此降级或拒绝。
23. **向前兼容**：新增命令只能追加；字段布局变更必须 bump 版本号并同步 `docs/api-spec.md`；旧版本请求必须仍可被识别（返回兼容错误码而非解析失败）。

## 4. 文档约束

- `docs/api-spec.md`：协议/接口唯一权威来源，改代码前先改它。
- `docs/design-details.md`：模块内部实现细节，实现与重构时必须保持同步。
- `docs/architecture.md`：架构总览与里程碑，重大方案变更时更新。
- `docs/deployment.md`：部署配置、运维操作、备份与灾难恢复（配置项变更时同步）。
- `docs/performance-baseline.md`：性能基线，bench 结果回归对照（每版本刷新）。
- `CHANGELOG.md`：变更日志，每个发布版本必须追加条目。
- `tools/RELEASE_CHECKLIST.md`：发布检查清单，打 tag 前逐项确认。

## 5. 工程流程与 CI/CD

24. **代码审查强制**：所有改动走 PR；至少 1 人评审通过方可合入。涉及并发原语、内存序、存储格式的改动，须由熟悉本项目并发约束（§3.2）的评审人复核。
25. **CI 流水线**（GitHub Actions 等价物，本地可复现）：
    - 步骤 1：`clang-format --dry-run --Werror`（格式门禁）
    - 步骤 2：`cmake -S . -B build -DMQ_BUILD_TESTS=ON` + `cmake --build build -j`
    - 步骤 3：`ctest --test-dir build --output-on-failure`
    - 步骤 4：ASan/UBSan 构建跑单测（并发与存储路径必跑）
    - 步骤 5（可选/定时）：`bench` 结果与 `docs/` 性能基线对比，回归即失败
26. **提交信息规范**：`<type>(<scope>): <subject>`，type ∈ {feat, fix, docs, refactor, test, chore}，scope 为模块名（storage/network/protocol/server/client/core）；主题用中文，≤ 50 字。
27. **发布流程**：版本号按 `vMAJOR.MINOR.PATCH` 递增；`tools/` 发布检查清单（构建/测试/文档/版本号/变更日志）通过后方可打 tag。

## 6. 常用命令

```bash
# 配置（Debug，含测试）
cmake -S . -B build -DMQ_BUILD_TESTS=ON
# 构建
cmake --build build -j
# 测试
ctest --test-dir build --output-on-failure
# 格式化检查
clang-format --dry-run --Werror $(git ls-files '*.h' '*.cpp')
# 压测基准（bench）
cmake -S . -B build -DMQ_BUILD_BENCH=ON
```

## 7. 完成定义（DoD）

一个功能/模块"完成"须同时满足：
1. 编译通过、测试全绿；
2. 协议/文档已同步；
3. 性能指标达到 PRD 目标（可测项）；
4. 无未处理的异步/竞态隐患（通过 code review 或 sanitizer 验证）。

## 8. 当前状态与路线

- 当前已完成 P1 MVP、P2 高可用和 P3 核心生产化安全能力；TLS、客户端 Token/ACL、复制认证、协议 Fuzz 和本地质量门禁均已验证。
- 后续按 `docs/architecture.md` §9 推进生产专项：远程 CI 实际运行、证书轮换与密钥托管、长期容量/稳定性压测、快照与日志截断，以及多身份 Token/ACL 管理。
