# mq-project 性能基线报告

> 状态: P3 Linux 生产矩阵样本已记录，正式硬件基线待确认 · 2026-08-24 更新
> 基准环境与复现步骤见 §4；数据刷新后更新版本号与日期。

## 1. 基线摘要

| 指标 | 目标（PRD §5.1） | 实测 | 结论 |
|------|------------------|------|------|
| 生产吞吐 (batch=1000, 3 分区) | ≥ 10w TPS | 256 B 单连接 257,637 TPS；4 KiB/10 连接 147,034 TPS | 达到样本目标 |
| 消费吞吐（多分区并行） | ≥ 10w TPS | TBD | - |
| 生产 p50 / p99 | < 1ms / < 5ms | 256 B/batch=1000 单连接 p50 约 2.7us，p99 约 4.4us | 达到样本目标 |
| 消费 p99 | < 10ms | TBD | - |
| 5w 连接内存占用/连接 | ≤ 1KB 空闲 | TBD | - |
| 写入放大 | ≤ 2× | TBD | - |

## 2. 分场景明细

| 场景 | 说明 | 吞吐 | p50 | p99 | p999 |
|------|------|------|-----|-----|------|
| 生产 batch=1 | 256 B/1 连接/3 分区 | 6,267 TPS | 约 141us | 约 442us | 约 922us |
| 生产 batch=100 | 256 B/1 连接/3 分区 | 227,089 TPS | 约 3.3us | 约 7.6us | 约 10.4us |
| 生产 batch=1000 | 256 B/1 连接/3 分区 | 257,637 TPS | 约 2.8us | 约 4.4us | 约 5.5us |
| 生产 batch=1000 | 4096 B/10 连接/3 分区 | 147,034 TPS | 约 62.8us | 约 167.8us | 约 210.2us |
| 消费顺序读 | 热段 | TBD | | | |
| 消费随机 offset | 冷段/索引定位 | TBD | | | |
| 长轮询消费 | 空队列挂起 | TBD | | | |

## 3. 稳定性与资源

| 项 | 实测 |
|----|------|
| 5w 连接稳定运行（时长） | TBD |
| 段滚动（1GiB）下的吞吐波动 | TBD |
| 清理线程运行期积压 | TBD |
| 内存峰值 / 写缓冲水位 | TBD |
| 关闭抖动（重启恢复耗时） | TBD |

## 4. 复现环境与方法

- 硬件：CPU / 内存 / 磁盘（NVMe SSD）型号与数量。
- OS：内核版本（epoll / io_uring）。
- 压测命令：`./build/mq_bench produce --topic test --messages 1000000 --size 256 --connections 10 --batch 1000 --partitions 3`。
- 参数含义：`--connections` 为并发 TCP Producer 数，`--batch` 为单次请求消息数，`--partitions` 为 Topic 分区数；工具启动时会打印实际生效参数。
- 方法：预热 → 打点 60s → 取稳定区间；每场景跑 3 次取中位数。

### 4.2 WSL2 验证记录

- 环境：WSL2 Ubuntu，Linux 5.10.16.3-microsoft-standard-WSL2，GCC 13.3.0。
- 普通 Debug 构建、ASan/UBSan 构建均通过。
- 普通 CTest：10/10 通过；ASan/UBSan CTest：10/10 通过。
- TCP 冒烟：1000 条消息、2 个连接、batch=100、3 个分区，TPS 2824.74；Broker 收到 SIGINT 后正常退出。
- 该结果用于 Linux 功能验证，不作为正式性能基线；正式基线仍需按下方矩阵记录硬件、磁盘和重复轮次。

### 4.1 Linux 标准基准矩阵

推荐使用仓库脚本自动执行生产矩阵并生成 CSV 原始结果：

```bash
chmod +x tools/run_bench_matrix.sh
tools/run_bench_matrix.sh build-linux 1000000 3
```

脚本要求 Broker 已运行在 `127.0.0.1:9092`，也可通过 `MQ_BENCH_HOST` 和 `MQ_BENCH_PORT` 覆盖；结果写入当前目录 `bench-results-<timestamp>.csv`。CSV 中保留每次运行的完整工具输出，正式填表时取每个场景 3 次的中位数。

推荐使用仓库脚本自动执行生产矩阵并生成 CSV 原始结果：

```bash
chmod +x tools/run_bench_matrix.sh
tools/run_bench_matrix.sh build-linux 1000000 3
```

脚本要求 Broker 已运行在 `127.0.0.1:9092`，也可通过 `MQ_BENCH_HOST` 和 `MQ_BENCH_PORT` 覆盖；结果写入当前目录 `bench-results-<timestamp>.csv`。CSV 中保留每次运行的完整工具输出，正式填表时取每个场景 3 次的中位数。

```bash
./build/mq_bench produce --topic bench_256 --messages 1000000 --size 256 --connections 1 --batch 1 --partitions 3
./build/mq_bench produce --topic bench_256 --messages 1000000 --size 256 --connections 1 --batch 100 --partitions 3
./build/mq_bench produce --topic bench_256 --messages 1000000 --size 256 --connections 1 --batch 1000 --partitions 3
./build/mq_bench produce --topic bench_256_c10 --messages 1000000 --size 256 --connections 10 --batch 1000 --partitions 3
./build/mq_bench produce --topic bench_4k_c10 --messages 100000 --size 4096 --connections 10 --batch 1000 --partitions 3
./build/mq_bench consume --topic <retained_topic> --messages 1000000 --connections 3 --partitions 3 --group bench_group
```

上述样本来自 WSL2 Linux 2026-08-23 的 3 轮矩阵结果，256 B/10 连接场景首轮存在明显预热差异，因此表格采用三轮中位数，不能视为固定硬件承诺。正式回归使用 `tools/check_bench_matrix.sh` 校验每个场景的完整轮次及保守 TPS/p99 门槛。

矩阵结果校验：

```bash
tools/check_bench_matrix.sh bench-results-<timestamp>.csv 3
```

## 5. 回归判定

- CI 中 bench 结果与本表对比（`AGENTS.md` §5.25 步骤 5）：
  - 吞吐 < 基线 80% 或 p99 > 基线 2× → 判定回归失败，需定位后再合入。
