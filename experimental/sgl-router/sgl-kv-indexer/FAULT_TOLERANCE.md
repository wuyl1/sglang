# KV Indexer 容错设计

## 1. 目标

KV Indexer 是内存中的软状态。Indexer、Bridge 或 Worker 重启，以及 ZMQ 丢事件，都可能让索引与 Worker 的真实 KV 缓存不一致。

本设计只保证：

- 故障后索引最终恢复；
- 恢复期间推理仍可正常执行，最多降低缓存命中率；
- 不引入 WAL、强一致复制或两阶段提交。

## 2. 故障与恢复

所有故障最终都走同一条路径：**从 Publisher 获取全量 Snapshot，通过现有 apply 接口重建索引。**

| 故障 | 如何发现 | 如何恢复 |
| --- | --- | --- |
| Indexer 重启 | Bridge 建立新的 gRPC 连接 | 全量 Snapshot |
| Bridge 重启 | Bridge 启动时没有 cursor | 全量 Snapshot |
| Worker 重启 | 首条 CLEAR 或 seq 回退 | 清除旧 placement；必要时全量 Snapshot |
| ZMQ 丢事件 | seq gap 或事件解码失败 | 全量 Snapshot |
| 无法被事件发现的漂移 | 周期定时器 | 全量 Snapshot |

### Worker 重启

Worker 重启后 KV 缓存为空，但 Indexer 仍可能保留旧 placement。Publisher 必须保证：任何会重置该 replica KV 状态的重启之后，第一条事件是全 tier CLEAR。

Bridge 同时检查 seq 回退。若 CLEAR 丢失且回退未被发现，周期 Snapshot 会最终修复。

### Bridge 停机

Bridge 停机期间 Indexer 可能保留陈旧 placement。Router 仍会过滤不健康的 Worker，因此不会影响推理正确性。Bridge 恢复后通过全量 Snapshot 收敛。

## 3. Snapshot 契约

首版只需要全量接口：

```text
GetSnapshot(replica_key) -> {
    actions: [CLEAR + REPORT],
    last_seq: uint64,
}
```

Snapshot 包含：

1. 每个 tier 一条 `CLEAR_ALL_AT_TIER`；
2. 若干 `REPORT`，携带 hash、tier、component mask 和 block size；
3. 与 Snapshot 对应的 `last_seq`。

### Snapshot 与 `last_seq` 必须原子一致

```text
snapshot.actions
    == Publisher 处理完所有 seq <= snapshot.last_seq 后的完整状态
```

Publisher 在同一个临界区或串行事件循环中：

1. 完整更新 placement mirror；
2. 推进 `last_seq`；
3. 原子复制 `(mirror, last_seq)`。

序列化可以在复制完成后进行。禁止分别读取 mirror 和 `last_seq`，否则 Bridge 可能永久漏掉事件。

不需要 barrier：

- 幂等 apply 允许 Snapshot 与实时事件重复；
- 原子 `last_seq` 保证两者之间没有遗漏。

## 4. Bridge 恢复流程

Bridge 按 replica 在内存中保存已成功应用的最大 seq：

```text
触发恢复
  -> 获取 Snapshot
  -> 按序应用 CLEAR 和 REPORT
  -> 全部成功后 cursor = last_seq
  -> 丢弃 buffered seq <= cursor
  -> 按序应用 buffered seq > cursor
```

Snapshot 通过现有 `ApplyExternalKvBatch` 应用，不增加 Indexer RPC。REPORT、REVOKE 和 CLEAR 都必须保持幂等。

Bridge 必须保持单 writer、严格按序、同一时刻只有一个在途 apply。

### Snapshot 中途失败

- 尚未应用任何 Snapshot 分片：保留旧索引，退避重试；
- CLEAR 或 REPORT 已部分应用：保留当前部分索引，不推进 cursor；
- 从 CLEAR 开始重新应用完整 Snapshot；
- 最后一个分片成功后才能推进 cursor。

部分索引只会降低缓存命中率。首版不为保留旧索引增加 staging 或 readiness gate。

### 订阅与写入解耦

当前 Bridge 在等待 Indexer apply 时会停止读取 ZMQ，可能在 SUB HWM 上自行制造丢包。

Bridge 改成两个任务：

- 订阅任务持续读取 ZMQ，并写入有界 pending 队列；
- 写入任务从队列取数据，严格按序发送给 Indexer。

队列满时主动触发 Snapshot，而不是等待 ZMQ 静默丢包。

### 错误处理

- 临时错误：full-jitter 退避并重试；
- 协议或配置错误：告警并停止重试该非法请求，但不退出进程；
- seq gap、回退、解码失败：触发 Snapshot；
- Snapshot 查询失败：保留当前索引并继续退避；
- 普通 live batch 分片失败：只重试该分片。

## 5. 周期恢复

事件驱动恢复无法发现尾部丢失或静默漂移。每个 replica 定期执行全量 Snapshot：

- 默认间隔 10 分钟；
- 使用 full jitter 打散；
- 一个周期内已经完成过全量恢复则跳过；
- 与其他恢复共用并发限制。

周期 Snapshot 只能保证 Indexer 与 Publisher mirror 一致，不能验证 mirror 是否与 Worker 真实缓存一致。

## 6. 资源限制

| 资源 | 默认值 |
| --- | --- |
| pending 队列 | 4096 batches，同时限制 256 MiB |
| apply 批大小 | 沿用现有 16,384 hashes/request |
| Snapshot 查询 | 30 秒超时，最多重试 3 次 |
| 并发恢复 | 4–8 个 replica |
| 周期恢复 | 10 分钟 + full jitter |

pending 队列至少要覆盖一次恢复期间产生的事件：

```text
queue_capacity >= peak_event_rate * recovery_time
```

连续溢出必须告警，否则可能形成“恢复 → 队列溢出 → 再次恢复”的循环。

## 7. 改动范围

### Publisher

1. 维护包含 tier、component mask 和 block size 的 placement mirror；
2. 原子生成 Snapshot 与 `last_seq`；
3. Worker 生命周期重置后首先发送全 tier CLEAR。

### Bridge

1. 解耦 ZMQ 订阅与 Indexer 写入；
2. 跟踪 seq 和 cursor；
3. 在启动、重连、gap、回退、解码失败和定时到期时恢复；
4. 正确处理 Snapshot 中途失败与 fail-open。

### 不修改

- Indexer 数据结构；
- 现有 `ApplyExternalKvBatch` proto；
- Router。

## 8. 验收标准

- Indexer 重启后，Bridge 自动全量恢复；
- Bridge 重启后，索引最终收敛；
- Worker 重启后，旧 placement 全部消失；
- 人为丢失 ZMQ 事件后，检测 gap 并恢复；
- Indexer 慢响应时，Bridge 仍持续读取 ZMQ；
- Snapshot 与 `last_seq` 在并发更新下保持一致；
- Snapshot 在任意分片失败时 cursor 都不推进，重试后最终收敛；
- 重复 Snapshot 和重叠事件不会改变最终结果；
- Snapshot 查询失败时 Bridge 不清空索引、不退出进程；
- 高事件率下恢复不会因 pending 队列溢出而反复重启。

## 9. 不做

首版不做：

- barrier；
- readiness gate 或恢复状态机；
- publisher epoch；
- Snapshot 专用 Indexer RPC；
- staging、commit 或 WAL；
- lease、writer fencing；
- 短 gap replay；
- Indexer 持久化或复制。

只有在现有方案出现实际瓶颈或无法接受的错误窗口时，再引入对应机制。
