# KV Indexer 容错设计

## 1. 核心问题

KV Indexer 是内存软状态，下面四类故障会让索引与 Worker 的真实 KV 缓存不一致：

| 故障 | 问题 |
| --- | --- |
| Indexer 重启 | 内存索引全部丢失 |
| Bridge 重启 | 停机期间的事件没有转发 |
| Worker 重启 | KV 缓存已清空，但 Indexer 仍保留旧 placement |
| ZMQ 丢事件 | Bridge 收到的实时事件不完整 |

索引不一致只影响缓存命中率，不影响推理正确性。设计目标是故障后最终恢复，而不是让索引成为强一致服务。

## 2. 恢复方案

| 故障 | 检测信号 | 恢复方式 |
| --- | --- | --- |
| Indexer 重启 | Bridge 建立新的 gRPC 连接 | Snapshot |
| Bridge 重启 | Bridge 启动时没有已应用序号 | Snapshot |
| Worker 重启 | epoch 变化，legacy 下为首条 CLEAR 或 seq 回退 | Snapshot |
| ZMQ 丢事件 | seq gap | Replay，补不齐回落 Snapshot |
| 事件解码失败 | payload 或 seq 无法解析 | Snapshot |

无论走 Replay、Snapshot 还是实时流，所有事件都由 Bridge 的同一个 writer 严格按序写入 Indexer，同一时刻只有一个 apply 在执行。Bridge 同时记录**已应用序号**，即确认写入 Indexer 的最大 seq。

### Gap 补齐

SGLang 的 `ZmqEventPublisher` 自带 ROUTER replay 端点和 `buffer_steps` 个 batch 的历史缓冲，默认 10000。通过 `--kv-events-config` 的 `replay_endpoint` 打开即可，不需要改 SGLang 代码。

```text
向 replay_endpoint 发送 8 字节大端 start_seq
  -> 按序应用回放的 batch
  -> 收到 END_SEQ 标记后结束
  -> 继续应用队列中的实时事件
```

replay 不含 CLEAR，索引不会被擦掉重建。因此不需要按 gap 大小设阈值：gap 出现就先试 replay，补不齐再回落 Snapshot。解码失败时 replay 只会返回同一份坏数据，直接取 Snapshot。

三个必须处理的细节：

- 请求的 seq 已被缓冲挤掉时 Publisher 不报错，只会少发事件并照常发 END_SEQ。Bridge 必须校验回放从请求值开始且全程连续，否则回落 Snapshot；
- `END_SEQ` 是 `-1` 的 8 字节大端补码，按 u64 解出来是 `u64::MAX`，必须特判。否则已应用序号会被推到顶，之后所有事件都被当成旧事件丢弃；
- Publisher 的 replay 与发布共用一个线程，且异常只打日志，请求可能无人应答。Bridge 必须设超时并回落 Snapshot。

为避免 Indexer 写入变慢导致 ZMQ 接收阻塞，Bridge 用两个任务分别负责订阅和写入，中间通过有界队列连接。队列满说明 Bridge 自己丢了事件，也按 gap 处理；容量按峰值事件率和最长恢复时间配置。

### Snapshot

Snapshot 由上游 [#34407](https://github.com/sgl-project/sglang/pull/34407) 提供，我们不自己实现。它按 DP replica 暴露 `snapshot_endpoint`，每块传输 4096 条记录，在我们 16384 的 apply 上限之内。

#### 已经满足：干净的切面

上游把序号分配、事件应用和快照捕获放在同一个线程串行，因此 snapshot 内容与水位天然一致。

Bridge 先订阅并缓存实时事件，再获取 Snapshot，等到实时流中出现与 Snapshot 匹配的 barrier 后开始恢复：

```text
按序应用 Snapshot 的 CLEAR + REPORT
  -> 全部成功后，已应用序号 = barrier_seq
  -> 丢弃队列中 seq <= 已应用序号 的事件
  -> 按序应用剩余事件
```

barrier 证明实时订阅已经到达 Snapshot 的切面，避免订阅刚建立时漏掉切面之后的事件。Snapshot 与实时流重叠的部分可以重复应用，因为 REPORT 是整体替换、REVOKE 是移除、CLEAR 是清空，重复执行不改变结果。如果以后把 component mask 改成增量合并，就必须重新设计恢复流程。

#### 当前 PR 还缺什么

**还缺 tier、block size 和 component mask，这是当前唯一的硬阻塞。**

- tier：当前 mirror 只按 block hash 保存，无法表示同一个 block 同时存在于 GPU 和 CPU，也无法只删除其中一层；
- block size：重建后无法恢复 `token_count`；
- component mask：重建后无法判断 block 包含哪些 KV component。

上游需要把 mirror 的 key 改为 `(hash, medium)`，并在 snapshot 记录中带上 `medium`、`block_size` 和 component metadata；`BlockRemoved` 也要按 medium 删除。协议已有 version，可以通过新版本增加这些字段。

#### 适配项

Bridge 需要增加 ZMQ snapshot 客户端，并解析、等待上游的 barrier 帧。PR 还没合、CI 未过，现在推动上游补齐字段成本最低。

Snapshot 获取或 barrier 等待失败时保留当前索引并退避重试。Snapshot 已部分写入 Indexer 时不推进已应用序号，从 CLEAR 开始重试；期间索引可能为空或不完整，但只影响缓存命中率。重试使用 full jitter，避免 Indexer 重启后所有 Bridge 同时恢复。

### Worker 生命周期

Worker 重启清空 KV 缓存后，如果新 REPORT 叠加到旧 placement 上，会形成无法自愈的幽灵记录，所以必须识别新的生命周期。

优先使用 #34407 的 `epoch`：它按 DP replica 的生命周期划分，并随每条消息携带。epoch 变化就重新同步该 replica；同一 epoch 内收到不大于已应用序号的事件，只当作 replay 与实时流重叠产生的重复事件丢弃。

对没有 epoch 的 legacy publisher，重启后第一条事件必须是全 tier CLEAR，并以 seq 回退作为兜底信号。

## 3. 改动与验收

### 改动

Publisher：

1. replay 已经实现，配置 `--kv-events-config` 的 `replay_endpoint`；
2. snapshot 与 epoch 依赖 #34407，并需上游补齐 tier、block size 和 component mask。

Bridge：

1. 解耦 ZMQ 订阅与 Indexer 写入；
2. 用单 writer 严格按序写入，跟踪 seq 和已应用序号，并解析 epoch；
3. gap 时先 replay，补不齐或超时回落 Snapshot；
4. 在启动、重连、epoch 变化、legacy seq 回退或解码失败时取 Snapshot；
5. 增加 snapshot 的 ZMQ 客户端，等待匹配的 barrier，并处理分块与中途失败。

首版不修改 Indexer 数据结构、现有 apply proto 和 Router，也不增加 WAL、staging、readiness gate、lease 或周期恢复。

遗留问题：索引没有淘汰和 TTL。正常的 ZMQ 丢事件会由 replay 或 Snapshot 修复；如果 Publisher 本身漏发 REVOKE 或 mirror 出错，残留条目会持续累积且没有任何信号。先暴露索引条目数指标，再决定是否需要回收机制。

### 验收

- Indexer、Bridge 或 Worker 重启后索引最终收敛；
- gap 落在 replay 缓冲内时用 replay 补齐，索引不被清空重建；
- 请求的 seq 已被 replay 缓冲挤掉时回落 Snapshot 并最终收敛；
- `END_SEQ` 不被当成真实 seq，已应用序号不会被推到 `u64::MAX`；
- replay 无人应答时超时回落 Snapshot；
- legacy Worker 重启后 seq 归零时 Bridge 立即触发恢复，而不是把新事件当成旧事件丢弃；
- epoch 变化后该 replica 重新同步，旧 placement 不残留；
- Snapshot 重建后 tier、component mask 和 `token_count` 与重建前一致；
- Indexer 慢响应时 Bridge 仍持续读取 ZMQ；
- Snapshot 与其水位在并发更新下保持原子一致；
- 未收到匹配的 barrier 时不切换到实时流，并退避重试；
- Snapshot 任意分片失败时已应用序号不推进，重试后最终收敛；
- 重复 Snapshot 和重叠事件不改变最终结果；
- Snapshot 获取失败时 Bridge 不清空索引、不退出；
- 高事件率下不会形成“恢复 → 队列溢出 → 再次恢复”的循环。
