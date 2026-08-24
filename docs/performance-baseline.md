# mq-project 性能基线报告

> 状态: P3 VM16 Linux 生产与消费基线已记录 · 2026-08-24 更新
> 基准环境与复现步骤见 §4；数据刷新后更新版本号与日期。

## 1. 基线摘要

| 指标 | 目标（PRD §5.1） | 实测 | 结论 |
|------|------------------|------|------|
| 生产吞吐 (batch=1000, 3 分区) | ≥ 10w TPS | VM16：256 B/单连接 605,479 TPS；4 KiB/10 连接 137,965 TPS | 达到样本目标 |
| 消费吞吐（多分区并行） | ≥ 10w TPS | VM16：1,000,000 条、723,902 TPS | 达到目标 |
| 生产 p50 / p99 | < 1ms / < 5ms | VM16：256 B/batch=1000 单连接 p50 约 1.46us，p99 约 2.66us | 达到样本目标 |
| 消费 p99 | < 10ms | VM16：0.401us；p999 为 998.266us | 达到目标，存在少量长尾 |
| 5w 连接内存占用/连接 | ≤ 1KB 空闲 | TBD | - |
| 写入放大 | ≤ 2× | TBD | - |

## 2. 分场景明细

| 场景 | 说明 | 吞吐 | p50 | p99 | p999 |
|------|------|------|-----|-----|------|
| 生产 batch=1 | 256 B/1 连接/3 分区，VM16 | 5,079 TPS | 约 177us | 约 709us | 约 1,380us |
| 生产 batch=100 | 256 B/1 连接/3 分区，VM16 | 374,668 TPS | 约 2.28us | 约 8.81us | 约 17.64us |
| 生产 batch=1000 | 256 B/1 连接/3 分区，VM16 | 605,479 TPS | 约 1.46us | 约 2.66us | 约 4.49us |
| 生产 batch=1000 | 256 B/10 连接/3 分区，VM16 | 830,343 TPS | 约 10.77us | 约 32.17us | 约 46.71us |
| 生产 effective batch=251 | 4096 B/10 连接/3 分区，VM16 | 137,965 TPS | 约 66.56us | 约 211.49us | 约 443.78us |
| 消费顺序读 | WSL2 热段，1,000 条/3 分区/3 连接 | 4,804 TPS | 约 349us | 约 1,181us | 约 30,338us |
| 消费顺序读 | VM16 热段，1,000,000 条/3 分区/3 连接 | 723,902 TPS | 约 0.07us | 约 0.401us | 约 998.266us |
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

### 4.3 VM16 Linux 生产矩阵记录

- 环境：VMware VM16 Linux Release，项目位于 VM 本地 Linux 磁盘 `/home/lzh/mq-project`；Broker `127.0.0.1:9092`。
- 原始结果：`bench-results-20260824-203936.csv`。
- 以上表格取每个场景 3 轮中位数；5 个生产场景均完成 1,000,000 条消息，矩阵回归门禁通过。
- 消费基线已完成；执行命令为：

```bash
./build-vm16/mq_bench produce --topic vm16_consume_baseline --messages 1000000 --size 256 --connections 3 --batch 1000 --partitions 3
./build-vm16/mq_bench consume --topic vm16_consume_baseline --group vm16_bench_group --messages 1000000 --connections 3 --partitions 3
```

- 2026-08-24 已修复大 Fetch 响应超过 64KiB 网络写缓冲、bench 幂等 ID 跨场景复用、逐条提交位点和 SDK 丢弃批量 Fetch 响应的问题；VM16 使用修复后构建完成 1,000,000 条消费验证。

## 5. 回归判定

- CI 中 bench 结果与本表对比（`AGENTS.md` §5.25 步骤 5）：
  - 吞吐 < 基线 80% 或 p99 > 基线 2× → 判定回归失败，需定位后再合入。
