# mq-project 发布检查清单

> 用于发布 `vMAJOR.MINOR.PATCH` tag 前的逐项确认（`AGENTS.md` §5.27）。
> 任一检查不通过不得打 tag。

## 0. 版本与范围

- [ ] 版本号已按 semver 递增（vMAJOR.MINOR.PATCH）
- [ ] 变更范围与 `CHANGELOG.md` 本版本条目一致

## 1. 构建

- [ ] `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release` 成功
- [ ] `cmake --build build -j` 成功（无警告新增）
- [ ] Release + Debug 两种配置均可构建

## 2. 测试

- [ ] `cmake -S . -B build -DMQ_BUILD_TESTS=ON` + 构建成功
- [ ] `ctest --test-dir build --output-on-failure` 全绿
- [ ] ASan/UBSan 构建下单元测试全绿（并发与存储路径必跑）
- [ ] 存储/协议格式相关改动：崩溃恢复（kill -9）集成测通过

## 3. 文档同步

- [ ] 改动涉及协议/存储格式 → `docs/api-spec.md` / `docs/design-details.md` 已同步
- [ ] 配置项变更 → `docs/deployment.md` 已同步
- [ ] `CHANGELOG.md` 已补充本版本条目
- [ ] `docs/architecture.md`（如有架构变更）已更新

## 4. 性能

- [ ] 压测结果刷新至 `docs/performance-baseline.md`
- [ ] 与上一版本基线对比无回归（吞吐 ≥ 80% 基线，p99 ≤ 2× 基线）
- [ ] `tools/check_bench_matrix.sh bench-results-<timestamp>.csv 3` 通过
- [ ] 性能敏感代码改动已进 `bench/`

## 5. 质量与安全

- [ ] `clang-format --dry-run --Werror` 干净
- [ ] 无未合入评审的 PR（涉及并发/内存序/存储格式的改动已由专人复核）
- [ ] 无密钥/凭据/本地绝对路径入库；`.gitignore` 正确
- [ ] 依赖清单无新增未评审第三方库

## 6. 发布动作

- [ ] 打 tag：`git tag vX.Y.Z`（附注 tag，写明变更摘要）
- [ ] 发布物：源码 tag + 构建产物说明 + 部署文档入口
- [ ] 通知运维：配置变更、升级步骤、回滚方案
