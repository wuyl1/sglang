# KV Indexer 容错设计

- 状态：Draft
- 适用架构：`SGLang Publisher -> Bridge -> gRPC -> process-local Indexer -> Router`
- 参考实现：[KV Indexer PR #33370](https://github.com/sgl-project/sglang/pull/33370)、[Worker Snapshot PR #34407](https://github.com/sgl-project/sglang/pull/34407)
- 相关工作：[Snapshot RFC #33394](https://github.com/sgl-project/sglang/issues/33394)、[Replay #32729](https://github.com/sgl-project/sglang/issues/32729)

## 1. 背景与目标

Indexer 是进程内内存后端：重启即丢全部 placement；Bridge 当前只重连、不补数据；`seq` 仅用于日志；ZMQ gap、停机期间事件和 worker 失联都不可恢复。

索引是 advisory 的。placement 陈旧或缺失只影响缓存命中率，不影响推理正确性：Router 会把 Indexer 结果与健康候选集求交，没有可用信号时降级到最小负载路由。

容错目标是：

1. Indexer、Bridge 或 Worker 重启后最终恢复；
2. ZMQ 事件缺失后最终收敛；
3. 状态不确定时不退出进程、不无限增长资源；
4. 不为 advisory 索引引入 WAL、两阶段提交或强一致复制。

## 2. 四类实际故障

判断严重性的标准是索引偏向哪一侧：

- **子集**：该有的 placement 没有，只会低估前缀并造成缓存未命中；
- **超集**：不该有的 placement 仍存在，会给 Router 陈旧信号。

Worker 重启会产生长期且不自愈的超集，是最需要做对的一类。Bridge 停机、漏掉 REVOKE、snapshot 尚未追平也会产生超集，但这些窗口会在 Bridge 恢复、gap 修复或周期 re-baseline 后收敛。

### 2.1 Indexer 进程挂了

**现象：** 内存状态全部丢失。Bridge 的 gRPC 连接断开，重连后若继续发送增量，只能得到不完整索引。

**处理：** 任何新建立的 gRPC 连接都强制一次全量 snapshot。进程死亡必然导致 TCP/HTTP2 连接断开，因此不需要 Indexer 状态机、实例握手或启动版本号。

网络抖动导致的重连也会多做一次 snapshot，首版接受。若实测过于频繁，再增加 Indexer 进程启动标识。

### 2.2 Bridge 挂了

**现象：** Bridge 停机期间无人转发事件，Indexer 保留旧 placement。

**处理：** Bridge 启动时本地 cursor 为空，强制全量 snapshot。与 Indexer 重启使用同一条恢复路径。

**残留：** Bridge 停机期间可能返回陈旧 placement。Router 的 health filtering 会排除已下线 worker；对仍健康的 worker，后果是缓存命中率下降而非推理错误。

### 2.3 Worker 挂了

#### 挂了不回来

旧 placement 可能滞留，但 Router health filtering 不会将请求发送给已下线 worker。首版接受，不增加 lease。

#### 挂了又起来

KV 缓存已清空，但 seq 从 0 重新开始。若不清除旧状态，新生命周期的 REPORT 会叠加到旧 placement 上，而旧块永远不会收到 REVOKE，形成长期幽灵 placement。

**处理：** Publisher 必须保证，任何重置该 replica KV 状态的事件之后，第一条事件是全 tier CLEAR。该要求覆盖 TP、PP、attention rank 重启，而不只是 metadata publisher 线程重建。

兜底信号：

- `received_seq < expected_seq`：检测 seq 回退；
- snapshot 响应表明 Bridge cursor 比 Publisher 更新：强制全量 snapshot；
- 周期 re-baseline：封顶 CLEAR 丢失后的错误窗口。

### 2.4 ZMQ 事件缺失

ZMQ PUB 是 fire-and-forget，HWM 满时会丢事件。

**处理：**

1. 检测到 `received_seq > expected_seq` 时触发全量 snapshot；
2. Bridge 的订阅与写入解耦，避免 apply 阻塞时停止读取 ZMQ、由 Bridge 自己制造 gap；
3. 周期 re-baseline 修复没有后续事件可暴露的尾部丢失。

首版任何 gap 都全量恢复。短 gap replay 仅作为未来性能优化。

## 3. 统一恢复模型

恢复不是“发布基线再做两阶段提交”，而是“从 Publisher 获取一个权威 snapshot，再走现有幂等 apply 路径”。

统一触发：

| 故障 | 触发信号 | 恢复动作 |
| --- | --- | --- |
| Indexer 重启 | 新 gRPC 连接 | 全量 snapshot |
| Bridge 重启 | cursor 为空 | 全量 snapshot |
| Worker 重启 | CLEAR、seq 回退 | 应用 CLEAR，必要时全量 snapshot |
| ZMQ 缺失 | seq gap、解码失败 | 全量 snapshot |
| 无信号漂移 | 周期定时器 | 全量 snapshot |

## 4. Publisher Snapshot

### 4.1 首版接口

首版只需要全量 snapshot：

```text
GetSnapshot(replica_key) -> {
    actions: [CLEAR + REPORT events],
    last_seq: uint64,
}
```

Snapshot 内容：

1. 每个 tier 一条 `CLEAR_ALL_AT_TIER`；
2. 若干 `REPORT`，携带 hash、tier、component mask 和 block size；
3. `last_seq` 表示该 snapshot 对应的 Publisher 事件水位。

Publisher placement mirror 至少按 `(block_hash, tier)` 建模：

- `REPORT` 是 `(replica, tier, block)` 的 REPLACE；
- `REVOKE` 和 `CLEAR` 同步更新 mirror；
- 某 tier 的移除不得删除其他 tier 的 placement；
- component-aware worker 的 snapshot 不得缺失 component、tier 或 block size。

### 4.2 Snapshot 与 `last_seq` 原子一致

这是恢复正确性的硬前提：

```text
snapshot.actions
    == Publisher 处理完所有 seq <= snapshot.last_seq 后的完整 placement 状态
```

Publisher 必须在同一个临界区或串行事件循环内取得 mirror 的不可变副本和 `last_seq`：

1. 处理事件 seq=`S`：先完整更新 mirror，再把水位推进到 `S`；
2. 处理 snapshot 请求：原子读取 `(mirror snapshot, last_seq)`；
3. 释放锁后再序列化不可变 snapshot，避免长时间阻塞事件处理。

禁止：

- 先读取 `last_seq`，再无锁遍历变化中的 mirror；
- 先复制 mirror，再读取更晚的 `last_seq`。

Bridge 只有在全部 snapshot 分片成功应用后，才能把 cursor 推进到 `last_seq` 并丢弃 `seq <= last_seq` 的 buffered event。

不需要 barrier，但不能没有原子水位：

- 事件幂等解决 snapshot 与 live stream 的重复；
- 原子水位保证 snapshot 与 live stream 之间没有遗漏。

## 5. Snapshot 复用现有 Apply 路径

Indexer 和现有 apply proto 不需要修改。现有协议已经支持：

```proto
ACTION_REPORT
ACTION_REVOKE
ACTION_CLEAR_ALL_AT_TIER
```

Snapshot 转换为普通 batch：

```text
batch 0: CLEAR_ALL_AT_TIER for every tier
batch 1..n: REPORT(hashes, component_masks, block_sizes)
```

Bridge 使用现有 `split_apply_request` 切分，并通过 `ApplyExternalKvBatch` 严格按序发送。

### 5.1 幂等性不变量

- REPORT 是 per-`(replica, tier, block)` 的 REPLACE；
- REVOKE 是移除；
- CLEAR 是清空；
- 三类 action 重复应用均不改变最终状态。

Bridge 必须保持单 writer、严格按序、同一时刻只有一个在途 apply。不得改成多个 batch 流水线发送。

### 5.2 Snapshot 中途失败

直接复用 apply 路径意味着恢复不是原子替换。CLEAR 一旦成功，旧 placement 已被删除；后续 REPORT 失败时，Indexer 会停留在空或部分状态。

明确语义：

- 尚未应用任何 snapshot 分片时失败：保留旧 placement，fail-open，退避重试；
- 至少一个 snapshot 分片成功后失败：接受当前部分索引作为安全降级，不推进 cursor；
- 从第一条 CLEAR 开始重试完整 snapshot；
- 最后一个分片成功后才推进 cursor 到 `last_seq`，再排空 buffered live event。

不承诺应用中途失败还能保留旧状态。若未来必须提供该保证，才考虑 staging/readiness gate；首版不做。

部分状态只会降低缓存命中率，不影响推理正确性。

## 6. Bridge 行为

### 6.1 订阅与写入解耦

当前 `run_session` 在同一个循环内先 `subscriber.recv()`，再等待 apply。apply 阻塞或退避时，Bridge 会停止读取 ZMQ，最终在 SUB HWM 上丢事件。

改成两个任务：

- **订阅任务：** 持续读取 ZMQ，写入有界 pending 队列；
- **写入任务：** 从 pending 队列取 batch，单 writer、单在途发送。

队列满时主动放弃并触发 snapshot，而不是等待 ZMQ 被动丢包。

### 6.2 Cursor 与恢复流程

Bridge 按 replica 在内存中维护“已成功应用的最大 seq”：

```text
任一恢复触发
  -> GetSnapshot(replica)
  -> 按序应用 CLEAR + REPORT
  -> 全部成功后 cursor = snapshot.last_seq
  -> 丢弃 buffered seq <= cursor
  -> 按序应用 buffered seq > cursor
```

Bridge 重启后 cursor 为空，自动全量恢复。任一 snapshot 分片失败时 cursor 保持原值。

### 6.3 恢复触发

- 新建立 gRPC 连接；
- Bridge 启动或首次发现 replica；
- seq gap；
- seq 回退；
- 事件解码失败；
- pending 队列溢出；
- subscriber 永久退出或连续接收错误；
- 周期 re-baseline。

普通 live batch 的某个 RPC 分片临时失败时，仅重试该分片，不触发 snapshot。

### 6.4 错误分类与 Fail-open

- `UNAVAILABLE / DEADLINE_EXCEEDED / CANCELLED / RESOURCE_EXHAUSTED`：临时错误，原样退避重试；
- `INVALID_ARGUMENT / UNIMPLEMENTED / UNAUTHENTICATED / PERMISSION_DENIED`：协议或配置错误，停止重试同一非法请求，告警，但不退出进程；
- 其他错误：重新获取 snapshot。

在应用 snapshot 前查询持续失败：

- 保留现有 placement；
- 告警并 full-jitter 退避；
- 不清空、不退出。

Snapshot 已开始应用后失败：

- 保留当前部分状态；
- 不推进 cursor；
- 从 CLEAR 重试完整 snapshot。

## 7. 周期 Re-baseline

信号驱动恢复无法发现：

- CLEAR 丢失且 seq 流看起来连续；
- 空闲 worker 的尾部事件丢失；
- Bridge/Indexer 侧的静默状态漂移。

因此每个 replica 以较长间隔触发全量 snapshot：

- 起点 10 分钟；
- 跨 replica full jitter 打散；
- 一个周期内已经完成其他全量恢复，则跳过该次；
- 与其他恢复共用并发配额；
- 指标标记 `trigger="periodic"`。

周期 snapshot 只能让 Indexer 与 Publisher mirror 重新一致，不能证明 Publisher mirror 与真实 worker cache 一致。若要校验 mirror 本身，必须与独立来源（例如 worker 本地 cache tree）比较。

## 8. 有界资源

| 资源 | 起点 |
| --- | --- |
| pending 队列 | 4096 batches，同时限制 256 MiB |
| snapshot apply 批大小 | 沿用现有 16,384 hashes/request |
| snapshot 查询 deadline / 重试 | 30 s / 3 次 |
| 并发恢复 replica | 4–8 |
| re-baseline 间隔 | 10 min + full jitter |

队列容量按恢复耗时计算：

```text
queue_capacity >= peak_batch_rate * recovery_deadline
```

如果稳定事件率乘以恢复耗时超过队列容量，每次恢复都会溢出并再次触发恢复，形成不收敛循环。连续溢出必须告警并调整容量或恢复耗时。

## 9. 可观测性

- `kv_bridge_cursor{worker,rank}`;
- `kv_bridge_recovery_total{trigger,result}`;
- `kv_bridge_snapshot_records`;
- `kv_bridge_recovery_duration_seconds`;
- `kv_bridge_pending_batches`;
- `kv_bridge_pending_bytes`;
- `kv_bridge_pending_overflow_total`;
- `kv_bridge_snapshot_query_failures_total`;
- `kv_bridge_fail_open_active`.

告警覆盖：

- pending overflow 反复出现；
- fail-open 长期持续；
- snapshot 连续失败；
- connect-triggered recovery 异常频繁；
- snapshot records 异常增长。

## 10. 工作项

| # | 工作项 | 组件 |
| --- | --- | --- |
| 1 | 原子获取 placement snapshot 与 `last_seq` | Publisher |
| 2 | Worker 生命周期重置后首条事件为全 tier CLEAR | Publisher |
| 3 | 订阅与写入解耦，增加有界 pending 队列 | Bridge |
| 4 | cursor 跟踪与统一 snapshot 恢复流程 | Bridge |
| 5 | 恢复触发、错误分类、fail-open、周期 re-baseline | Bridge |

Indexer、现有 apply proto 和 Router 首版均不修改。

## 11. 验收标准

### 按故障验收

| 故障 | 验收 |
| --- | --- |
| Indexer 重启 | Bridge 在新连接上全量恢复，不需要重启 Bridge |
| Bridge 重启 | 全量恢复后收敛，停机期间模型请求仍成功 |
| Worker 重启 | seq 从 0 开始后旧 placement 最终全部消失 |
| ZMQ 丢事件 | 人为丢 seq 后检测并恢复 |
| Bridge 自造丢包 | Indexer 慢响应时 Bridge 仍持续读取 ZMQ |
| 冷启动 | Worker 已有缓存时启动 Bridge/Indexer，能够恢复路由 |
| 恢复风暴 | 多个 Bridge 同时恢复时并发受限 |
| 无信号漂移 | 一个 re-baseline 周期内自动恢复 |

### 核心测试

- Snapshot 与 `last_seq` 在并发 mirror 更新下保持原子一致；
- 禁止“先读水位后复制 mirror”和“先复制 mirror 后读水位”；
- Snapshot 加 `last_seq + 1` 起的事件不得遗漏；
- Snapshot 在 CLEAR 后、任一 REPORT 分片后失败，cursor 均不推进；
- 从 CLEAR 重试后最终收敛；
- Snapshot 重复应用和重叠事件重放结果不变；
- CLEAR 删除旧生命周期全部 placement；
- seq gap、回退和解码失败触发恢复；
- 普通 live batch 分片失败只重试该分片；
- Snapshot 查询失败时不清空、不退出。

### 核心不变量

1. Action 幂等：任意重叠区间重放后收敛到同一状态；
2. Snapshot 原子水位：actions 精确表示处理完所有 `seq <= last_seq` 后的状态；
3. cursor 只在完整 snapshot 成功后推进；
4. 失败时不丢弃尚未确认的 buffered event；
5. Worker 生命周期重置后，旧 placement 最终全部消失；
6. 只要 Publisher、Bridge 和 Indexer 最终稳定，状态最终收敛。

## 12. 明确不做

- barrier / 精确拼接点；
- readiness gate、replica 状态机；
- `publisher_epoch` 及 apply 侧 epoch 校验；
- snapshot 专用 Indexer RPC、staging、commit、`sync_id`；
- Indexer 侧 seq gate、digest、WAL；
- lease、writer fencing；
- 短 gap replay；
- 持久化或多 Indexer 状态复制。

重新考虑条件：

- 事件不再幂等：恢复 barrier 或 Indexer seq gate；
- 必须在恢复失败时保留旧状态：增加 staging/readiness gate；
- CLEAR 丢失窗口不可接受：增加 `publisher_epoch`；
- 全量 snapshot 成为瓶颈：增加短 gap replay；
- 索引成为强一致依赖：重新评估 WAL 与复制。

## 13. 结论

四类故障统一为一条恢复路径：

```text
Indexer 重启 / Bridge 重启 / seq gap / 周期定时
    -> 获取与 last_seq 原子一致的全量 snapshot
    -> 通过现有幂等 apply 路径按序应用
    -> 全部成功后推进 cursor 并追平 buffered event
```

Worker 生命周期重置由第一条全 tier CLEAR 表达。

落地范围是 Publisher 两项、Bridge 三项；Indexer、现有 apply proto 和 Router 首版不修改。
