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

# 只验证死信重投相关行为（筛选/幂等/反压/崩溃恢复/再次失败）
go test -race -count=1 -run 'Replay' ./...
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
[replay]    op=replay-xxx group=g key=incident-01 matched=2 claimed=2 status=created
[replay]    op=replay-xxx id=t-0#3 status=enqueued new-offset=4
[replay]    op=replay-xxx id=t-0#5 status=backpressure offset=10 reason=queue-full
[replay]    op=replay-xxx id=t-0#5 status=recovered-existing new-offset=10
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
| `TestReplayFilterAndRedeliver` | 按分区/位移/原因/时间范围筛选死信重投；副本追加尾部、溯源完整、顺序不乱 |
| `TestReplayIdempotent` / `TestReplayConcurrentIdempotent` | 同幂等键重复/并发触发只恢复同一操作，绝不重复入队 |
| `TestReplayBackpressureAndResume` / `TestReplayRestartRecoversCompleteOp` | 容量满 `ErrQueueFull` 反压、续跑补齐、重启恢复 pending/complete 操作 |
| `TestReplayCrashWindowsInternal` | 白盒：账本与认领/日志写入之间各崩溃窗口恢复后不丢不重 |
| `TestReplayFailAgain` | 重投副本重获完整重试，再次超限再进死信且可再次重投（round 递增） |
| `TestReplayOrderingRegression` / `TestReplayInflightBackpressure` | 位移不回退/不复活、独占栅栏不回归、重投消费受在途窗口反压 |
| `TestConcurrentConsumers` / `TestStressConcurrentConsume` | 并发消费不丢不重（8 分区×50 条×6 消费者） |

## 2. 消息模型

- 主题由若干 **分区（TopicPartition）** 组成；每个分区是一条**只追加日志**。
- 位移 `offset` 从 0 开始单调递增，严格等于写入顺序；同分区消费顺序与生产顺序一致。
- 磁盘布局（均在 `Config.DataDir` 下）：

  ```
  logs/<topic>-<partition>.log   # 分区追加日志（重投副本也追加在这里的尾部）
  groups/<group>.json            # 消费组代次与已提交位移
  retries.json                   # 全部组的待重试项、死信记录与重投认领标记
  replays.json                   # 死信重投操作账本（幂等、崩溃恢复、可查询）
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
- 重启后：分区日志从文件重建；位移与代次从 `groups/*.json` 恢复；
  待重试与死信从 `retries.json` 恢复。成员与独占关系属于租约态，重启后失效，
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

  > **兼容性说明**：对“消费位移”而言死信仍是终态——原位移不会被复活、提交位
  > 不回退。第 5 节的**死信重投**提供的是另起一条新日志记录的“再投递”能力，
  > 它不改变上述终态保证，只是在分区尾部追加一条携带完整溯源的新消息。

## 5. 死信重投（Dead-Letter Replay）

针对“死信只能人工导出再手动发回，容易漏、容易重、无法交代”的问题，新增一层
**死信重投**：把某消费组里筛选出的一条或一批死信，重新投回正常消费流程。

### 5.1 核心语义：追加为新消息，而非复活原位移

重投**不是**把原来的死信位移重新激活，而是把该死信的消息体作为**一条新记录
追加到同分区日志尾部**。由此天然满足全部既有约束：

- **位移不回退、不重复**：原死信位移保持终态，已经成功消费并提交过的其他消息
  绝不会被再投一遍；重投副本拿到的是日志尾部的新位移。
- **顺序不被打乱**：重投副本排在同分区所有现有消息之后，同分区后续消息不会
  跑到它前面，它也不会插队到任何消息之前。一批重投按 `(分区, 原死信位移)`
  的稳定顺序串行入队。
- **独占与租约不变**：重投副本就是普通消息，投递给谁、谁能 Ack/Nack，完全
  沿用消费组独占、心跳租约与会话代次栅栏；同分区同一时刻仍只有一个消费者处理。
- **重新获得完整重试机会**：重投副本的 `Attempt` 从 1 重新计数，再次失败仍按
  同一套确定性退避（`BaseDelay * Multiplier^(attempt-1)`、`MaxAttempts`）处理；
  再次超过上限，会作为一条**新的死信记录**再次进死信，并且还能再次被重投。

### 5.2 溯源：失败原因、次数与原始来源完整保留

每条重投副本都在消息头里写入保留前缀 `x-mq-replay-*` 的溯源信息，可用
`mq.ParseReplayProvenance(delivery.Record.Headers)` 解析出：

| 字段 | 含义 |
| --- | --- |
| `OriginTopic/OriginPartition/OriginOffset` | 最初那条死信的位置（不是上一跳） |
| `OriginAttempts` | 当初进死信前累计的失败次数 |
| `OriginReasons` | 当初进死信前的完整失败原因链 |
| `OriginEnqueued` / `DeadAt` | 原始入队时间 / 当初进死信时间 |
| `Round` | 重投代次（第 1 次重投为 1，对重投副本再次重投递增） |
| `OpID` | 把它投回来的重投操作 ID（审计与幂等排查用） |

对“重投副本再次失败、再次进死信、再次重投”的链路，溯源始终指向**最初来源**，
代次递增，新一轮失败原因则记录在新的 `DLQEntry.Reasons` 里，整条链路可复盘。
业务自定义消息头不要使用 `x-mq-replay-` 前缀（重投时该前缀下的旧头会被覆盖）。

### 5.3 筛选与 API

```go
// 按条件挑选某组的死信并触发重投；idemKey 由调用方提供，用作幂等键。
res, err := b.ReplayDeadLetters("order-workers", "ticket-2026-09-28-01",
    mq.ReplayFilter{
        Topic:          "orders",                 // 按主题
        Partitions:     []int{0, 3},              // 按分区
        Offsets:        []int64{42},              // 或精确指定原死信位移
        ReasonContains: "timeout",                // 失败原因链包含子串
        DeadFrom:       t0, DeadTo: t1,           // 按进死信的时间范围（含端点）
        IncludeReplayed: false,                   // 默认排除已重投过的死信
    })
// res.Matched 命中数；res.EnqueuedNow 本次实际入队数；res.Operation 可持久查询

b.ResumeReplay(res.Operation.OpID)              // 反压解除后续跑，或重启后续跑
op, _   := b.GetReplay(opID)                    // 查询单个操作（重启后亦可）
ops     := b.ListReplays("order-workers")       // 列出该组全部重投操作
prov, ok := mq.ParseReplayProvenance(d.Record.Headers) // 解析重投副本溯源
```

- 筛选字段之间为“与”关系；零值字段不过滤。空筛选表示该组全部未重投死信。
- `(group, idemKey)` 相同的重复调用（含多个调用方并发、进程重启后的重试）只会
  恢复并继续**同一个操作**，绝不重复入队。传空 `idemKey` 会自动生成，但跨
  重启幂等需要调用方自己保存并复用稳定的键，因此**建议显式传入业务侧单号**。
- 没有任何死信命中筛选时返回 `ErrNothingToReplay`（可用 `errors.Is` 区分）。

### 5.4 并发、容量与崩溃下的正确性

- **反压**：重投入队受分区容量（`PartCapacity`）约束；入队后的消费同样受
  `MaxInflight` 在途窗口约束。装不下时 `ReplayDeadLetters/ResumeReplay`
  立即返回 `ErrQueueFull`——已入队部分不回滚，操作停在 `pending`，**不静默丢弃、
  不重复入队**；容量恢复后用 `ResumeReplay(opID)`（或同幂等键再次调用）续跑。
- **并发幂等**：所有重投操作在 Broker 串行区完成，并以
  “**重投账本先落盘 → 再认领死信 → 预留位移 → 追加日志（fsync）→ 回写已入队**”
  的顺序推进。同一条死信被认领后默认不会再被其他操作挑选
  （`IncludeReplayed=true` 可显式再次挑选，且会生成新操作、新副本）。
- **崩溃恢复**：重投账本持久化在 `replays.json`（临时文件 + 原子 rename），
  死信的认领标记持久化在 `retries.json`。无论在哪个步骤崩溃，重启后：
  - 账本在、认领未完成：同幂等键恢复操作，未认领条目重新认领并入队一次；
  - 日志已写入、“已入队”标记未完成：续跑时扫描分区日志尾部，按
    `(opID, 原始位置)` 认回已 fsync 的副本，**绝不追加第二条**；
  - 已完成操作与全部条目的新位移均可通过 `GetReplay/ListReplays` 查询。

### 5.5 与旧“死信即终态”说法的兼容范围

- **保持不变**：原死信位移仍是消费终态；提交位不回退；顺序/独占/租约/退避/
  `MaxAttempts` 规则对重投副本一视同仁；不使用重投 API 的旧行为完全不变。
- **新增能力**：死信记录上额外带认领标记（`ReplayedOpID/ReplayedNewOffset`），
  且分区尾部可能出现带头 `x-mq-replay-*` 的重投副本；它们是**新位移的新消息**，
  不是旧位移复活，也不会被计入旧位移的提交/重试状态。

## 6. 主要 API

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

// 排查/补救：按失败原因把某组死信重投回正常消费流程（建议传入稳定的业务单号）
res, err := b.ReplayDeadLetters("order-workers", "incident-2026-09-28",
    mq.ReplayFilter{ReasonContains: "timeout"})
if errors.Is(err, mq.ErrQueueFull) {
    // 容量反压：稍后续跑，已入队部分不会重复
    res, err = b.ResumeReplay(res.Operation.OpID)
}
```

反压与可观测：`LogStats()` 返回每分区 `Count/Capacity/Used/HighWatermark/Full`；
`Assignments(group)` 返回当前分区独占关系；`Step()` 在使用可控时钟时推进
租约过期处理（生产环境由 `Poll/Heartbeat` 内部顺带执行）；死信重投操作可用
`GetReplay(opID)` / `ListReplays(group)` 查询进度与每条的新位移。

## 7. 包结构

| 文件 | 职责 |
| --- | --- |
| `mq/message.go` | 消息/记录/投递模型、哨兵错误、消息标识 |
| `mq/log.go` | 分区追加日志：落盘格式、fsync、启动截断恢复、水位统计 |
| `mq/group.go` | 成员心跳租约、会话代次栅栏、确定性独占分配、位移持久化与校验 |
| `mq/retry.go` | 确定性退避策略、待重试调度、死信与原因链持久化、死信重投认领标记 |
| `mq/replay.go` | 死信重投：筛选条件、溯源消息头、重投操作账本（幂等/崩溃恢复/可查询） |
| `mq/broker.go` | 门面：投递窗口/投递游标、回收重投、Ack/Nack/Commit、重投联动 |
