# mq-project 接口/协议设计

> 版本: v0.1 · 更新: 2026-08-19
> 协议字段布局由 `src/protocol` 实现，本文件是唯一权威定义。

## 1. 传输层

- TCP 长连接，客户端可复用连接。
- 二进制协议，**length-prefixed framing**，每条消息独立完整。
- 大端字节序。

## 2. 帧格式

### 2.1 请求帧（客户端 → Broker）
```
偏移  长度  字段          说明
0     2     magic         0x4D51 ("MQ")
2     1     version       当前 1
3     1     command       Command 枚举值
4     8     request_id    客户端自增，响应回填
12    2     flags         保留/位标志（如压缩）
14    2     topic_len     Topic 名长度
16    n     topic         UTF-8 Topic 名
16+n  4     payload_len   载荷长度
20+n  m     payload       命令参数
```

### 2.2 响应帧（Broker → 客户端）
```
偏移  长度  字段          说明
0     2     magic         0x4D51 ("MQ")
2     1     version       1
3     1     status        Status 枚举值（0 = OK）
4     8     request_id    回填请求 ID
12    2     flags         保留
14    4     payload_len   载荷长度
18    m     payload       结果数据
```

## 3. 命令（Command 枚举）

| 值 | 命令 | 载荷（请求） | 载荷（响应） |
|----|------|--------------|--------------|
| 0x01 | CREATE_TOPIC | topic + partitions | 无 |
| 0x02 | DELETE_TOPIC | topic | 无 |
| 0x03 | LIST_TOPIC | 无 | topic 列表 |
| 0x10 | PRODUCE | partition + key + value | offset + partition |
| 0x20 | FETCH | partition + offset + max_bytes | 消息列表 |
| 0x21 | COMMIT_OFFSET | group + partition + offset | 无 |
| 0x30 | HEARTBEAT | group | 无 |

### 3.1 P1 MVP 载荷布局

除非另有说明，所有整数均为大端字节序，字符串均为 UTF-8 原始字节。`topic` 始终取自请求帧而非命令载荷。

| 命令 | 请求载荷 | 成功响应载荷 |
|------|----------|--------------|
| `CREATE_TOPIC` | `partition_count(4)`，范围 1~1024 | 空 |
| `LIST_TOPIC` | 空 | `count(4)`，重复 `topic_len(2) | topic | partition_count(4)` |
| `PRODUCE` | `partition(4) | key_len(2) | key | value_len(4) | value`；`partition=0xFFFFFFFF` 表示按 key 路由 | `partition(4) | offset(8)` |
| `FETCH` | `partition(4) | offset(8) | max_bytes(4)` | `count(4)`，重复 `offset(8) | timestamp_ms(8) | key_len(2) | key | value_len(4) | value` |

- `PRODUCE` 的 `key_len` 最大为 65535，`value_len` 范围为 1~1048576。
- `FETCH` 的 `max_bytes` 必须大于 0；若起始 offset 超出已提交范围，返回 `INVALID_OFFSET`。
- 当前 P1 实现只支持 `ack=1` 的本地写入确认；`ack=0` 和 `ack=all` 保留给复制模块。

## 4. 状态码（Status 枚举）

| 值 | 含义 |
|----|------|
| 0x00 | OK |
| 0x10 | BAD_REQUEST |
| 0x11 | UNKNOWN_TOPIC |
| 0x12 | TOPIC_EXISTS |
| 0x13 | INVALID_OFFSET |
| 0x14 | STORAGE_ERROR |
| 0x15 | VERSION_MISMATCH |
| 0x20 | INTERNAL_ERROR |

## 4.1 版本协商

- 握手帧中客户端携带 `min_version / max_version`；Broker 返回协商版本与自身版本。
- 版本不兼容 → `VERSION_MISMATCH` + `current_version`；客户端据此降级或拒绝。
- 新增命令只允许追加；字段布局变更必须 bump 版本号，旧版本请求返回兼容错误码而非解析失败。

## 5. 幂等与重试

- 客户端重复发送同一 `request_id` 时，Broker 返回上次结果（幂等表，近期滑动窗口）。
- 超时/网络错误由客户端重试，保证 at-least-once。

## 6. 变更流程

1. 修改本文件；
2. 同步 `src/protocol`（枚举、编解码）；
3. 同步 `docs/test-plan.md` 编解码用例。
