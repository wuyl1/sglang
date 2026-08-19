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
| Worker 重启 | epoch 变化，legacy 下为首条 CLEAR 或 seq 回退 | Snapshot |
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

### Snapshot

Snapshot 由上游 [#34407](https://github.com/sgl-project/sglang/pull/34407) 提供，我们不自己实现。它给 publisher 加了 `snapshot_endpoint`，按 DP replica 独立暴露，分块传输，每块 4096 条记录，在我们 16384 的 apply 上限之内。

#### 已经满足：干净的切面

snapshot 内容正好等于 Publisher 处理完水位之前所有事件之后的状态。上游把序号分配、事件应用和快照捕获放在同一个线程串行，这条由构造成立，不用额外论证。

有了它就不需要 barrier：和实时事件重叠的部分由幂等吸收，水位保证重叠之外没有缺口。这里的幂等指 REPORT 整体替换 `(replica, tier, block)`、REVOKE 移除、CLEAR 清空，重复应用不改变结果。它是省掉 barrier 的唯一依据，所以 apply 语义不能动——把 component mask 改成增量合并之类的改动，会让整个设计不报错地失效。

#### 还缺：snapshot 装不下我们的 placement 模型

**这是唯一的硬阻塞。** 我们的 `BlockRecord` 需要每个 block 的 `token_count`，以及每个 `(worker, tier)` 的 component mask。上游给的是：

```text
KVSnapshotBlock { parent_block_hash, block_hashes }
mirror: dict[block_hash, KVSnapshotBlock]
```

`BlockStored` 携带的 `medium`、`block_size` 和 metadata 在写入 mirror 时就被丢弃，`BlockRemoved` 不看 `medium`，直接按 hash 删。所以问题不是少几个字段，而是**这个 mirror 结构上就是单 tier 的**：同一个 hash 同时驻留在两层无法表示，从一层移除会连带抹掉另一层。而我们的 CLEAR 是 `CLEAR_ALL_AT_TIER`，按层清——两个模型对不上。

要推上游改四处：

1. mirror 按 `(hash, medium)` 组织；
2. 记录带上 `block_size` 与 component metadata；
3. `BlockRemoved` 只删匹配的 medium；
4. `KVSnapshotBlock` 相应加字段。

升级路径是通的：header 里已有 `version`，当前为 1，且这些结构是 `array_like`，末尾追加字段对老消费者兼容。

#### 适配项

不构成障碍，但要做：snapshot 走 ZMQ 端点而不是 gRPC，Bridge 要加个客户端；上游的 barrier（`barrier_seq`、`barrier_id`、`resume_seq`）我们只取 `barrier_seq` 当水位，但帧得能解；PR 还没合、CI 未过，落地前别当成稳定依赖，也意味着现在提上面那四处改动成本最低。

### Worker 生命周期

Worker 重启清空 KV 缓存后，如果新 REPORT 叠加到旧 placement 上，会形成无法自愈的幽灵记录，所以必须能识别出生命周期已经翻篇。

优先使用 #34407 的 `epoch`：它按 DP replica 的生命周期划分，并随每条消息挂在 topic 帧上（NUL 分隔追加，SUB 前缀过滤不受影响）。epoch 变化即触发该 replica 重新同步。这比依赖一条事件更可靠——CLEAR 是单条事件，可能丢失，而 epoch 在任意一条消息上都能看到。

对没有 epoch 的 legacy publisher，退回原有约定：重启后第一条事件必须是全 tier CLEAR，并以 seq 回退作为兜底信号。

### Bridge 行为

Bridge 在内存中维护一个**已应用序号**，即已经确认写入 Indexer 的最大 seq。它既用来判断队列里哪些事件是多余的，也是检测 seq gap 和 seq 回退的基准。

```text
获取 Snapshot
  -> 按序应用全部分片
  -> 已应用序号 = snapshot 水位
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

Publisher 侧不写新代码，两项能力都已存在或在途：

1. replay 已经实现，配置 `--kv-events-config` 的 `replay_endpoint` 即可；
2. snapshot 与 epoch 依赖 #34407 合入，并需确认 snapshot 携带 tier、component mask 和 `token_count`。

Bridge：

1. 解耦 ZMQ 订阅与 Indexer 写入；
2. 跟踪 seq 和已应用序号，解析 topic 帧中的 epoch；
3. gap 时先 replay，校验首条回放 seq，补不齐或超时回落 Snapshot；
4. 在启动、重连、epoch 变化、回退和解码失败时取 Snapshot；
5. 增加 snapshot 的 ZMQ 客户端，处理分块与中途失败。

首版不修改 Indexer 数据结构、现有 apply proto 和 Router，也不增加 WAL、staging、readiness gate、lease 或周期恢复。上游的 barrier 我们只取其水位，不实现 barrier 等待语义。

遗留问题：索引没有淘汰也没有 TTL，条目只有 REVOKE 和 CLEAR 两条出路。ZMQ 丢事件会被 gap 检测发现，replay 或 Snapshot 都会补上缺失的 REVOKE，不留残留；但 Publisher 漏发 REVOKE 或 mirror 与 Worker 真实状态不一致时，残留会持续累积且没有任何信号。先暴露索引条目数指标，再决定是否需要回收机制。

### 验收

- Indexer、Bridge 或 Worker 重启后索引最终收敛；
- gap 落在 replay 缓冲内时用 replay 补齐，索引不被清空重建；
- 请求的 seq 已被 replay 缓冲挤掉时回落 Snapshot 并最终收敛；
- `END_SEQ` 不被当成真实 seq，已应用序号不会被推到 `u64::MAX`；
- replay 无人应答时超时回落 Snapshot；
- Worker 重启后 seq 归零时 Bridge 立即触发恢复，而不是把新事件当成旧事件丢弃；
- epoch 变化后该 replica 重新同步，旧 placement 不残留；
- Snapshot 重建后 tier、component mask 和 `token_count` 与重建前一致；
- Indexer 慢响应时 Bridge 仍持续读取 ZMQ；
- Snapshot 与其水位在并发更新下保持原子一致；
- Snapshot 任意分片失败时已应用序号不推进，重试后最终收敛；
- 重复 Snapshot 和重叠事件不改变最终结果；
- Snapshot 查询失败时 Bridge 不清空索引、不退出；
- 高事件率下不会形成“恢复 → 队列溢出 → 再次恢复”的循环。
