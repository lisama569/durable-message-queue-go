# Durable Message Queue（本地持久化消息队列与消费组）

用本地磁盘模拟一套具备**分区有序日志、消费组独占、确定性重试与死信、崩溃重启恢复**
能力的消息队列。纯 Go 标准库实现，无外部依赖；线程安全，提供可控时钟以便确定性测试。

## 1. 快速开始与验证

```bash
# 全量单测（含 -race 竞态检测与 3 次重复执行）
go test -race -count=1 ./...
go test -race -count=3 ./...

# 覆盖率
go test -cover ./...
```

测试打开 `mq.Logger` 后会把判定过程打印到日志，每条日志都带**消息标识**
`topic-partition#offset`、**位移**与**判定依据**，例如：

```
[poll]      group=g1 consumer=c1 delivered=1 first=orders-0#0 ...
[nack]      id=t-0#0 status=retry-scheduled attempt=1 backoff=5ms reason="boom"
[poll-retry] id=t-0#0 attempt=2 reason=backoff-eligible
[commit]    tp=t-0 offset=2 status=rejected ... err=offset commit rejected: smaller ...
[reclaim]   group=g id=t-0#1 reason=owner-left
[rebalance] group=g gen=2 reason=lease-expired dead=[c1]
```

测试场景对应：

| 测试 | 覆盖要求 |
| --- | --- |
| `TestProduceConsumeOrderedAck` | 分区顺序稳定、在途不重复、Ack 幂等 |
| `TestRestartRedelivery` | 未确认消息重启后重投、已提交位移不重复 |
| `TestCommitOffsetValidation` | 位移回退 / 越界提交被明确拒绝且错误可区分、重复提交幂等 |
| `TestCapacityAndWatermark` | 容量满时 `ErrQueueFull` 反压、水位可观测，不静默丢弃 |
| `TestPartitionExclusivity` | 同组分区独占、非持有者操作被 `ErrNotOwner` 拒绝 |
| `TestLeaseExpiryRebalance` | 租约过期安全再分配、旧主迟到确认被会话栅栏拒绝 |
| `TestHeartbeatKeepsOwnership` | 正常心跳续租不被抢占 |
| `TestLeaveGroupReassign` | 主动离开立即释放分区、在途回收重投 |
| `TestRetryThenSuccess` / `TestDeadLetter` | 确定性退避重试、超限进死信并保留原因链 |
| `TestRetryBackoffDeterministic` | 退避数值确定、可复现 |
| `TestRestartDLQAndOffsets` | 重启后消息日志、位移、死信均从磁盘恢复 |
| `TestReplayByFilters` | 按分区/位移/时间范围/失败原因挑选重投、无候选报错、状态可查 |
| `TestReplayOrderingAndBackpressure` | 重投不回退位移、不重投已确认消息，顺序排在分区末尾之后 |
| `TestReplayFailsAgainGoesDLQWithOrigin` | 重投消息重置重试预算、再次失败进死信并保留 Origin 跨周期原因链 |
| `TestReplayCapacityBackpressure` | 容量不足整批 `ErrQueueFull`、零部分入队，恰好装得下的批次成功 |
| `TestReplayRestartRecovery` | 重启后重投完成态/来源头恢复，不重复追加、重投消息可继续消费 |
| `TestReplayConcurrentIdempotent` | 并发触发重投只有一个请求生效，不重不漏 |
| `TestReplayCrashReconcile` / `TestReplayInterleavedProduceReconcile` / `TestReplayPendingSurvivesRestart` | 崩溃窗口对账（已 fsync 不重投、缺失补齐）、与并发 Produce 交错、pending 跨重启续跑 |
| `TestConcurrentConsumers` / `TestStressConcurrentConsume` | 并发消费不丢不重（8 分区×50 条×6 消费者） |

## 2. 消息模型

- 主题由若干 **分区（TopicPartition）** 组成；每个分区是一条**只追加日志**。
- 位移 `offset` 从 0 开始单调递增，严格等于写入顺序；同分区消费顺序与生产顺序一致。
- 磁盘布局（均在 `Config.DataDir` 下）：

  ```
  logs/<topic>-<partition>.log   # 分区追加日志
  groups/<group>.json            # 消费组代次与已提交位移
  retries.json                   # 全部组的待重试项与死信记录
  replays.json                   # 全部组的死信重投请求与逐条进度
  ```

- 分区日志记录格式：`8 字节大端长度前缀 + 记录体`，每次 `Produce` 写入后 `fsync`。
  崩溃只会在文件尾部留下半条记录，启动扫描时对长度不足、位移不连续的尾部做
  **保守截断**，不会破坏已确认的前缀。
- 容量与水位：每分区有固定容量，达到 80% 时 `LogStats.HighWatermark=true`，
  达到 100% 时 `Full=true` 且 `Produce` 返回 `ErrQueueFull`——**明确反压而非丢弃**。

## 3. 消费组与位移语义

### 3.1 成员、租约与独占

- 消费者以 `JoinGroup(group, id)` 注册，得到句柄；同组内 id 不可重复。
- 成员持有**心跳租约**（`Config.LeaseTTL`）。客户端需周期性 `Heartbeat`；
  超过 TTL 视为崩溃，其分区被回收并可再分配。`LeaveGroup` 为主动释放。
- **分区独占**：一个分区同一时刻只属于一个消费者。分配为确定性规则
  （存活消费者 id 排序后按分区号取模），并对仍有效的持有者保持粘滞。
- **会话栅栏（fencing）**：成员离开/过期会推进全局代次，每个会话持有自己注册时的
  代次令牌。再平衡只隔离“离开/过期的那一个会话”，不波及其余存活成员；旧主即使
  “诈尸”般迟到提交/Ack，也会收到 `ErrStaleGeneration` / `ErrLeaseExpired` /
  `ErrUnknownConsumer`，杜绝两个消费者同时提交同一分区。
- 旧主未确认的在途消息在其离开/过期时**回收**，由新主重新投递，因此既不会双持，
  也不会静默跳过。

### 3.2 位移提交

- 提交语义采用“**下一条待消费位移**”（`logEndOffset` 合法）。
- `CommitOffset` 幂等，且明确区分两类非法提交：
  - 小于已提交值 → `ErrOffsetRolledBack`（回退拒绝）；
  - 大于日志末尾或为负 → `ErrOffsetOutOfRange`（越界拒绝）；
  - 与已提交值相同 → 成功（幂等）。
- `Ack` 为自动提交：Broker 维护每个「组×分区」的**投递游标**与在途表，
  只有“已投递且已确认（或已进死信）”的**连续前缀**才推进提交位。
  提交位永远不会越过投递游标，因此即使窗口分批取走消息，也不会把
  从未投递的消息当作已消费而跳过。

### 3.3 至少一次与重启恢复

- 整体为 **at-least-once**：未确认或确认落后于提交位的消息，在重启或易主后
  一定会被重新投递；消费成功的幂等性由业务侧结合消息标识保证。
- 重启后：分区日志从文件重建（Broker 启动时自动扫描 `logs/` 目录打开已有分区，
  无需再次 `CreateTopic`）；位移与代次从 `groups/*.json` 恢复；
  待重试、死信从 `retries.json` 恢复；待完成的重投请求从 `replays.json` 恢复并在
  下一次 `Poll`/重投调用时自动对账续跑。成员与独占关系属于租约态，重启后失效，
  消费者需重新加入。
- 所有元数据写入均采用“临时文件 + 原子 rename”。

## 4. 失败重试与死信

- 策略 `RetryPolicy`，退避**确定性、无随机抖动**，同样失败次数得到同样等待：

  ```
  backoff(attempt) = min(BaseDelay * Multiplier^(attempt-1), MaxDelay)
  ```

  默认 `MaxAttempts=3, BaseDelay=10ms, MaxDelay=1s, Multiplier=2`
  （10ms → 20ms → 40ms …）。
- 处理失败调用 `Nack(delivery, reason)`：
  - 未达上限：消息进入待重试，`NotBefore = 失败时刻 + backoff`；
    退避未到期前，同分区后续消息也不会越过它（顺序不被破坏、不重复入队）。
  - 达到上限：消息连同**完整快照**与**每次失败的原因链**进入死信，
    `Nack` 返回 `ErrRetryExhausted` 以便与“已安排重试”区分。
- 死信为终态：该位移从待重试集合移除，连续前缀提交可以越过它继续推进，
  分区不会被一条毒消息永久阻塞；`DeadLetters(group)` 可读取死信与原因链。

## 4.1 死信重投（Dead-Letter Replay）

`ReplayDeadLetters(group, filter)` 可把某消费组里一条或一批死信**重新投回正常
消费流程**。它与“进了死信就等同终态”的说法**兼容**：死信终态是相对“本次投递
生命周期”而言的——位移上的原始记录不再自动重投；重投不会复活原始位移，而是把
死信快照作为**一条全新记录追加到同一分区日志尾部**（新位移、`Attempt` 从 1 重新
计起、重新获得完整重试预算）。原始死信记录仍然保留并被标记 `Replayed=true`，
审计时 `DeadLetters` 仍能看到完整历史。

**来源保留与追溯**：重投记录携带 `x-mq-replay-*` 保留消息头（随分区日志
fsync），消费者可用 `delivery.ReplayInfo()` 读到上一跳的请求 ID、原始
`组×分区×位移`、原始失败次数、历次失败原因链、原始生产时间与重投跳数。
若重投消息再次超过重试上限，新死信的 `DLQEntry.Origin` 指向其原始来源，
`Reasons` 为跨周期完整原因链（上一跳原因在前），因此反复重投也能一路追溯。

**顺序、位移与租约**：

- 不回退已提交位移、不重投已确认消息：重投是日志尾部追加，提交位与投递游标
  只增不减；
- 同分区不超车：重投记录按追加顺序排在当时分区末尾之后，后续消息不会跑到它前面，
  它也不会插到现有未决消息前面；分区独占、心跳租约、会话代次栅栏对重投消息
  完全一致，同一分区同一时刻仍只有一个持有者；
- 重投消息再次失败时，走的是同一套确定性退避（无随机抖动）与 `MaxAttempts`
  上限，超限后再次进死信（`Origin.Hop` 递增），并可被再次重投。

**筛选条件**（`ReplayFilter`，零值字段不过滤）：`Topic`、`Partitions`、
`Offsets`（原始位移精确匹配）、`DeadAtFrom/DeadAtTo`（进死信时间，含端点）、
`FailureReason`（对历次失败原因链做子串匹配）。无候选时返回
`ErrNothingToReplay`，时间区间颠倒等返回 `ErrInvalidArgument`。

**反压**：

- 分区容量：追加前逐分区预检，任一分区装不下本批则**整批** `ErrQueueFull`，
  不部分入队、不静默丢；每分区由 `AppendMany` 在分区锁内一次性容量复核，
  与并发 `Produce` 交错也不会产生“半批”；
- 在途窗口：重投记录就是普通日志记录，投递时同样受 `MaxInflight` 约束，
  窗口满则留到下一次 `Poll`。

**幂等、并发与崩溃恢复**：

- 每次调用生成唯一请求 ID，请求先落盘 `replays.json` 再执行追加；
  已重投（`Replayed`）或已被其它**待完成**请求认领的死信不会被重复选中，
  多个调用方并发触发时只有一个请求成功，其余得到 `ErrNothingToReplay`；
- 执行按分区推进，每完成一个分区即 fsync 日志、标记死信、持久化进度；
  崩溃后重启（或 `ResumePendingReplays()`，`Poll`/`ReplayDeadLetters`
  会自动顺带执行）时，从请求记录的起点扫描日志，按“请求 ID + 原始位移”
  对账：已经 fsync 的条目不重复追加，缺失条目继续补齐——不重不漏；
- `ReplayStatus(group, requestID)` 查单条请求（`pending`/`completed`
  与每条的旧/新位移），`ListReplays(group)` 列出全部请求，状态重启后可恢复。

```go
// 把 g 组里“超时类”死信整批捞回来重投
res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{
    Topic:         "orders",
    Partitions:    []int{0, 1},
    FailureReason: "timeout",
})
// res.Items[i].OldOffset -> NewOffset；随后正常 Poll/Ack/Nack 即可
```

## 5. 主要 API

```go
b, _ := mq.NewBroker(mq.DefaultConfig("/tmp/mq-data"))
b.CreateTopic("orders", 4)
off, _ := b.Produce(mq.TopicPartition{Topic: "orders", Partition: 0},
    mq.Message{Key: []byte("k"), Value: []byte("v")})

c, _ := b.JoinGroup("order-workers", "worker-1")
for {
    ds, _ := b.Poll(c, 16)
    for _, d := range ds {
        if err := handle(d); err != nil {
            _ = b.Nack(c, d, err.Error()) // 确定性退避，超限进死信
        } else {
            _ = b.Ack(c, d)              // 幂等确认，连续前缀自动提交
        }
    }
}
```

反压与可观测：`LogStats()` 返回每分区 `Count/Capacity/Used/HighWatermark/Full`；
`Assignments(group)` 返回当前分区独占关系；`Step()` 在使用可控时钟时推进
租约过期处理（生产环境由 `Poll/Heartbeat` 内部顺带执行）。
死信重投：`ReplayDeadLetters`（筛选重投）、`ReplayStatus` / `ListReplays`
（状态查询，重启后可恢复）、`ResumePendingReplays`（主动续跑，通常无需调用）。

本地验证：

```bash
go test -race -count=1 ./...          # 全量单测（含竞态检测）
go test -run Replay -v ./mq/          # 只跑死信重投相关用例（含中文判定日志）
go test -cover ./...                  # 覆盖率
```

## 6. 包结构

| 文件 | 职责 |
| --- | --- |
| `mq/message.go` | 消息/记录/投递模型、哨兵错误、消息标识 |
| `mq/log.go` | 分区追加日志：落盘格式、fsync、启动截断恢复、水位统计 |
| `mq/group.go` | 成员心跳租约、会话代次栅栏、确定性独占分配、位移持久化与校验 |
| `mq/retry.go` | 确定性退避策略、待重试调度、死信与原因链持久化、死信筛选/重投标记 |
| `mq/replay.go` | 死信重投模型与筛选条件、重投请求持久化、来源信息（保留消息头）编解码 |
| `mq/replay_broker.go` | 重投门面：条件挑选、整批容量预检、追加执行与崩溃对账、认领/恢复/查询 |
| `mq/broker.go` | 门面：投递窗口/投递游标、回收重投、Ack/Nack/Commit 联动 |
