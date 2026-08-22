# mq-project 性能基线报告

> 状态: P1 实测进行中 · 2026-08-22 更新
> 基准环境与复现步骤见 §4；数据刷新后更新版本号与日期。

## 1. 基线摘要

| 指标 | 目标（PRD §5.1） | 实测 | 结论 |
|------|------------------|------|------|
| 生产吞吐 (batch=1000, 3 分区) | ≥ 10w TPS | TBD | - |
| 消费吞吐（多分区并行） | ≥ 10w TPS | TBD | - |
| 生产 p50 / p99 | < 1ms / < 5ms | TBD | - |
| 消费 p99 | < 10ms | TBD | - |
| 5w 连接内存占用/连接 | ≤ 1KB 空闲 | TBD | - |
| 写入放大 | ≤ 2× | TBD | - |

## 2. 分场景明细

| 场景 | 说明 | 吞吐 | p50 | p99 | p999 |
|------|------|------|-----|-----|------|
| 生产 batch=1 | 单条 | TBD | | | |
| 生产 batch=100 | | TBD | | | |
| 生产 batch=1000 | | TBD | | | |
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

截图中的 Linux 结果（旧版工具未实现 `--connections`，因此仅作为单连接参考）为：256 B 约 69,786 TPS；4096 B 约 13,100 TPS。修复后的标准矩阵结果应在此表中按机器、内核、CPU、内存、磁盘和运行次数补录。

## 5. 回归判定

- CI 中 bench 结果与本表对比（`AGENTS.md` §5.25 步骤 5）：
  - 吞吐 < 基线 80% 或 p99 > 基线 2× → 判定回归失败，需定位后再合入。
