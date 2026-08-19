# 变更日志

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。
版本号按 `vMAJOR.MINOR.PATCH` 递增（见 `AGENTS.md` §5.27）。

## [Unreleased]

### 骨架（P0）
- 建立仓库结构与 CMake 构建骨架。
- 文档体系：PRD、AGENTS、architecture、design-details、api-spec、test-plan、deployment。

### 规划中（P1 MVP）
- 存储引擎：WAL 段文件 + mmap 稀疏索引 + 崩溃恢复。
- 网络层：epoll Reactor（Main/Sub）+ 协议编解码 + 粘包处理。
- 并发：Worker 线程池 + 无锁 MPMC 队列。
- 核心：Topic/分区管理、生产/消费、位点提交。
- 客户端 SDK 与单测/集成测/压测。