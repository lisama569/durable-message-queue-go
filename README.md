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
| `TestConcurrentConsumers` / `TestStressConcurrentConsume` | 并发消费不丢不重（8 分区×50 条×6 消费者） |

## 2. 消息模型

- 主题由若干 **分区（TopicPartition）** 组成；每个分区是一条**只追加日志**。
- 位移 `offset` 从 0 开始单调递增，严格等于写入顺序；同分区消费顺序与生产顺序一致。
- 磁盘布局（均在 `Config.DataDir` 下）：

  ```
  logs/<topic>-<partition>.log   # 分区追加日志
  groups/<group>.json            # 消费组代次与已提交位移
  retries.json                   # 全部组的待重试项与死信记录
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

## 6. 包结构

| 文件 | 职责 |
| --- | --- |
| `mq/message.go` | 消息/记录/投递模型、哨兵错误、消息标识 |
| `mq/log.go` | 分区追加日志：落盘格式、fsync、启动截断恢复、水位统计 |
| `mq/group.go` | 成员心跳租约、会话代次栅栏、确定性独占分配、位移持久化与校验 |
| `mq/retry.go` | 确定性退避策略、待重试调度、死信与原因链持久化 |
| `mq/broker.go` | 门面：投递窗口/投递游标、回收重投、Ack/Nack/Commit 联动 |
