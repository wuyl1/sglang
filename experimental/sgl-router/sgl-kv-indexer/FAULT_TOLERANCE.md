# KV Indexer 容错设计

## 1. 核心问题

KV Indexer 是内存软状态，下面四类故障会让索引与 Worker 的真实 KV 缓存不一致：

| 故障 | 问题 |
| --- | --- |
| Indexer 重启 | 内存索引全部丢失 |
| Bridge 重启 | 停机期间的事件没有转发 |
| Worker 重启 | KV 缓存已清空，但 Indexer 仍保留旧 placement |
| ZMQ 丢事件 | REPORT、REVOKE 或 CLEAR 永久缺失 |

索引不一致只影响缓存命中率，不影响推理正确性。设计目标是故障后最终恢复，而不是让索引成为强一致服务。

## 2. 恢复方案

| 故障 | 检测信号 | 恢复方式 |
| --- | --- | --- |
| Indexer 重启 | Bridge 建立新的 gRPC 连接 | Snapshot |
| Bridge 重启 | Bridge 启动时没有已应用序号 | Snapshot |
| Worker 重启 | 首条 CLEAR 或 seq 回退 | Snapshot |
| ZMQ 丢事件 | seq gap 或事件解码失败 | Replay，补不齐回落 Snapshot |

ZMQ 丢事件只缺少数几个 batch，用 replay 补齐即可。其余三类故障要补的历史远超 replay 缓冲——索引全空、没有起点、缓存已清——只能取全量 Snapshot：

```text
获取 Publisher 全量 Snapshot
  -> 通过现有 ApplyExternalKvBatch 按序应用 CLEAR + REPORT
  -> 全部成功后推进已应用序号
  -> 应用 Snapshot 之后的实时事件
```

### Gap 补齐

SGLang 的 `ZmqEventPublisher` 自带 ROUTER replay 端点和 `buffer_steps` 个 batch 的历史缓冲，默认 10000。通过 `--kv-events-config` 的 `replay_endpoint` 打开即可，不需要改 SGLang 代码。

```text
向 replay_endpoint 发送 8 字节大端 start_seq
  -> 按序应用回放的 batch
  -> 收到 END_SEQ 标记后结束
  -> 继续应用队列中的实时事件
```

replay 不含 CLEAR，索引不会被擦掉重建。因此不需要按 gap 大小设阈值：gap 一出现就先试 replay，能不能补齐由缓冲决定。这一点对高负载尤其重要，ZMQ 丢包恰好发生在订阅端跟不上的时候，那正是最不该做全量重建的时刻。

三个必须处理的细节：

- 请求的 seq 已被缓冲挤掉时 Publisher 不报错，只会少发事件并照常发 END_SEQ。Bridge 必须校验第一条回放事件的 seq 等于请求值，不等说明洞还在，回落 Snapshot；
- `END_SEQ` 是 `-1` 的 8 字节大端补码，按 u64 解出来是 `u64::MAX`，必须特判。否则已应用序号会被推到顶，之后所有事件都被当成旧事件丢弃；
- Publisher 的 replay 与发布共用一个线程，且异常只打日志，请求可能无人应答。Bridge 必须设超时并回落 Snapshot。

### Snapshot 契约

```text
GetSnapshot(replica_key) -> {
    actions: [CLEAR + REPORT],
    last_seq: uint64,
}
```

Snapshot 必须包含 hash、tier、component mask 和 block size，并满足：

```text
snapshot.actions
    == Publisher 处理完所有 seq <= snapshot.last_seq 后的完整状态
```

Publisher 必须原子复制 placement mirror 和 `last_seq`。幂等 apply 解决 Snapshot 与实时事件的重复；原子 `last_seq` 保证两者之间没有遗漏，因此不需要 barrier。

这里的幂等指 REPORT 是 `(replica, tier, block)` 的整体替换、REVOKE 是移除、CLEAR 是清空，三者重复应用不改变结果。这是不需要 barrier 的唯一依据，改动 apply 语义（例如把 component mask 改成增量合并）会静默破坏本设计。

### Worker 生命周期

任何会清空该 replica KV 缓存的重启之后，Publisher 发送的第一条事件必须是全 tier CLEAR。否则新 REPORT 会叠加到旧 placement 上，形成无法自愈的幽灵记录。

### Bridge 行为

Bridge 在内存中维护一个**已应用序号**，即已经确认写入 Indexer 的最大 seq。它既用来判断队列里哪些事件是多余的，也是检测 seq gap 和 seq 回退的基准。

```text
获取 Snapshot
  -> 按序应用全部分片
  -> 已应用序号 = last_seq
  -> 丢弃队列中 seq <= 已应用序号 的事件
  -> 按序应用队列中 seq > 已应用序号 的事件
```

必须满足：

- 单 writer、严格按序、同一时刻只有一个在途 apply；
- ZMQ 订阅与 Indexer 写入使用两个任务，中间通过有界队列连接，避免慢 apply 阻塞 ZMQ 接收；
- `received_seq` 小于期望值一律按 Worker 重启处理并取 Snapshot，这条判断必须先于“丢弃 seq <= 已应用序号”，否则重启后 seq 归零的事件会被当成旧事件全部丢弃；
- Snapshot 查询失败时保留当前索引并退避重试，不清空、不退出；
- Snapshot 已部分应用时不推进已应用序号，从 CLEAR 开始重试完整 Snapshot，期间索引停在空或部分状态，只影响命中率；
- 普通实时 batch 的分片失败只重试该分片；
- 重试与重连使用 full jitter，避免 Indexer 重启后所有 Bridge 同时全量恢复。

队列满意味着 Bridge 自己丢了事件，按 gap 处理。队列容量至少覆盖一次恢复期间产生的事件：

```text
queue_capacity >= peak_event_rate * recovery_time
```

`recovery_time` 为 Snapshot 查询超时加应用耗时。首版取查询超时 30s、队列容量 4096 条，再按实测事件率调整。

## 3. 改动与验收

### 改动

Publisher：

1. 维护 placement mirror；
2. 原子生成 Snapshot 与 `last_seq`；
3. Worker 生命周期重置后首先发送全 tier CLEAR。

replay 已经实现，只需在 `--kv-events-config` 里配置 `replay_endpoint`。

Bridge：

1. 解耦 ZMQ 订阅与 Indexer 写入；
2. 跟踪 seq 和已应用序号；
3. gap 时先 replay，校验首条回放 seq，补不齐或超时回落 Snapshot；
4. 在启动、重连、回退和解码失败时取 Snapshot；
5. 正确处理 Snapshot 中途失败。

首版不修改 Indexer 数据结构、现有 apply proto 和 Router，也不增加 WAL、staging、readiness gate、publisher epoch、lease 或周期恢复。

遗留问题：索引没有淘汰也没有 TTL，条目只有 REVOKE 和 CLEAR 两条出路。ZMQ 丢事件会被 gap 检测发现，replay 或 Snapshot 都会补上缺失的 REVOKE，不留残留；但 Publisher 漏发 REVOKE 或 mirror 与 Worker 真实状态不一致时，残留会持续累积且没有任何信号。先暴露索引条目数指标，再决定是否需要回收机制。

### 验收

- Indexer、Bridge 或 Worker 重启后索引最终收敛；
- gap 落在 replay 缓冲内时用 replay 补齐，索引不被清空重建；
- 请求的 seq 已被 replay 缓冲挤掉时回落 Snapshot 并最终收敛；
- `END_SEQ` 不被当成真实 seq，已应用序号不会被推到 `u64::MAX`；
- replay 无人应答时超时回落 Snapshot；
- Worker 重启后 seq 归零时 Bridge 立即触发恢复，而不是把新事件当成旧事件丢弃；
- Indexer 慢响应时 Bridge 仍持续读取 ZMQ；
- Snapshot 与 `last_seq` 在并发更新下保持原子一致；
- Snapshot 任意分片失败时已应用序号不推进，重试后最终收敛；
- 重复 Snapshot 和重叠事件不改变最终结果；
- Snapshot 查询失败时 Bridge 不清空索引、不退出；
- 高事件率下不会形成“恢复 → 队列溢出 → 再次恢复”的循环。
