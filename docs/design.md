# 本地持久化消息队列与消费组 — 设计说明

一个纯 Go、零外部依赖、以本地磁盘模拟消息存储与位移管理的消息队列。
目标是让「并发消费、失败重试、进程重启」三类场景下的数据状态**一致且可解释**：
消息不丢不重（至少一次语义）、位移提交幂等、分区独占、失败可复现、死信可追溯。

## 1. 目录布局（磁盘即状态）

```
<RootDir>/
  logs/
    <topic>-<partition>.log        # 每分区一个 CRC32 保护的追加日志
  _state/
    topics.json                    # 主题 -> 分区数
    offsets.json                   # 消费组位移
    retries.json                   # 待重试消息（退避到期时间）
    dlq.json                       # 死信队列（保留原始失败原因）
    groups.json                    # 消费组成员、世代(generation)、分区分配
```

所有 `_state/*.json` 都通过「临时文件 + fsync + 原子 rename」写入，
崩溃最多丢失**最后一次未落盘的状态推进**，绝不会读到半写文件。

## 2. 消息模型与分区有序

- 每个主题固定若干分区；分区是**有序、只追加**的日志，位移(offset)从 `0` 开始单调递增。
- `Produce`：
  - `Key` 非空 → `FNV-1a(key) % 分区数`，同一 Key 稳定落到同一分区（同 Key 顺序稳定）；
  - `Key` 为空 → 按主题轮询(round-robin)。
- 单分区日志帧格式：`[8 字节帧长][4 字节 CRC32][payload]`，
  payload 含 offset、时间戳、Key、有序化的 headers、Body。
- 重启时全量扫描重建「offset → 字节位置」索引；遇到**撕裂/损坏尾帧**会截断到
  最后一个完整帧，已确认的数据不丢，半写尾帧不污染后续读取。

## 3. 消费组、独占与世代(generation)

- 消费者以 `JoinGroup(group, member)` 加入，协调器对「全体分区」做**确定性轮询分配**
  （分区与成员均排序后 `parts[i] -> members[i % n]`），结果可复现。
- **同一分区同一时刻只属于一个成员**。所有 Fetch/Ack/Nack/CommitOffset 都要通过
  `checkOwner`：成员存在、租约有效、世代匹配、且确为该分区属主，否则明确报错。
- 成员加入/退出/租约过期导致分配变化时，**世代号 +1**。持有旧世代的请求会收到
  `ErrStaleGeneration`，非属主收到 `ErrPartitionNotOwned`，错误彼此可区分。
- 租约：`Heartbeat` 续租；超过 `LeaseTTL` 未心跳的成员在下一次 join/heartbeat
  触发的再平衡中被剔除，其分区被安全回收。
- 再平衡回收分区时，协调器通过 revoke 钩子清空该组该分区的**在途(in-flight)标记**，
  使新属主能重新投递未确认消息——既不会两个消费者同时持有，也不会静默跳过。

## 4. 位移(offset)语义与幂等提交

位移语义为 **committed = 下一个待消费 offset**（初始 `0`）。

- `Ack(d)` 等价于提交 `d.Offset+1`；`CommitOffset(tp, off)` 可显式提交。
- 提交校验规则（拒绝时**绝不修改状态**，返回 `*OffsetError`，`Reason` 可区分）：
  - `off == 当前已提交` → **幂等 no-op**，成功；
  - `off < 当前已提交` → 拒绝，原因 `offset_regress`（禁止回退）；
  - `off < 0` 或 `off > logEnd` → 拒绝，原因 `offset_out_of_range`（禁止越界）。
- 位移按 **(group, topic, partition)** 持久化，不同消费组互不影响。
- **重启恢复**：已提交的位移存活；尚未提交（或提交落后）的消息重启后按
  committed offset 重新投递（at-least-once）。已 Ack 的消息不会再次投递。
- 重复 Ack / 重复 Commit 同一位移是安全的 no-op。

## 5. 失败重试与死信(DLQ)

- `Nack(d, reason)`：记录失败次数 `attempts` 与失败原因。
- 退避策略是接口 `Backoff.Delay(attempt)`，内置：
  - `ConstantBackoff{D}`：恒定延迟；
  - `LinearBackoff{Base}`：`Base * attempt`。
  - **刻意不引入随机抖动**，使重投时间点确定、可复现。
- 当 `attempts <= MaxRetries`：消息进入待重试集合，记录 `NotBefore=now+Delay(attempts)`。
  Fetch 遇到队头消息处于退避窗口时**就地等待（head-of-line blocking）**，
  不会越过它去取后面的消息（不静默跳过、不乱序）。
- 当失败次数耗尽（第 `MaxRetries+1` 次投递仍失败）：
  - 消息写入 **DLQ**，完整保留 Topic/分区/原始 offset/Key/Body/各次失败原因中的
    最终原因、尝试次数、生产时间与入死信时间；
  - DLQ 以 `(group, topic, partition, offset)` 去重，崩溃在「写 DLQ」与「推进位移」
    之间也不会重复入死信；
  - 随后仅向前推进 committed offset，分区不会被毒消息卡死。
- 重试状态按**消费组**隔离持久化，重启后未到期的重试仍继续生效。

## 6. 容量、背压与可观测水位

- 容量按**积压深度 lag = logEnd − 最慢消费组的 committed**计算（ack 会释放容量）。
  - `HighWatermark`：软水位，达到后置 `AtHighWatermark=true`，仅用于观测/告警；
  - `PartitionCap`：硬容量，达到后 `Produce` **阻塞等待**容量释放，
    直到消费者 Ack 腾出空间或 `ctx` 超时；等待中的消息**不会被丢弃或重复入队**，
    被 ctx 取消的生产不会写入日志。
- `PartitionStats` 暴露：LogStart/LogEnd、Lag、各 group 的 committed、
  在途数、待重试数、死信数、水位、是否触顶/满。
- `GroupStats` 暴露：世代号、成员数、各成员的分区分配。

## 7. 关键错误一览（可区分）

| 错误 | 触发条件 |
| --- | --- |
| `OffsetError{Reason: offset_regress}` | 提交位移小于当前已提交位移 |
| `OffsetError{Reason: offset_out_of_range}` | 提交位移 < 0 或 > logEnd |
| `ErrPartitionNotOwned` | 非分区属主尝试 Fetch/Ack/Nack/Commit |
| `ErrStaleGeneration` | 用旧世代号操作（分配已变化） |
| `ErrLeaseExpired` / `ErrUnknownMember` | 租约过期被剔除 / 成员不存在 |
| `ErrPartitionFull` | 达到硬容量（内部用于容量判断） |
| `ErrUnknownTopic` / `ErrUnknownPartition` | 主题或分区不存在 |

## 8. 验证方法

```bash
# 全量单测（含消息标识、位移、判定依据日志）
go test ./... -v

# 竞态检测
go test ./... -race
```

测试覆盖：

1. **持久化与恢复**：追加/读取一致、重启字节级一致、撕裂尾帧截断不丢数据；
2. **有序消费**：同 Key 固定分区、offset 顺序稳定；
3. **位移语义**：幂等提交、回退拒绝(`offset_regress`)、越界拒绝(`offset_out_of_range`)、
   拒绝后状态不变、重启后位移存活；
4. **并发与独占**：多消费者并发处理 60 条消息恰好不丢不重、非属主/旧世代被拒绝；
5. **租约与再平衡**：成员静默过期后分区安全回收，世代递增，被剔除成员无法提交；
6. **失败重试与死信**：确定性退避（精确到 50ms）、队头阻塞不跳过、超限进 DLQ
   且保留原始原因、死信后位移推进；
7. **重启恢复**：未 Ack 消息重启后重投、已提交位移处继续、剩余消息恰好消费一次；
8. **容量/背压**：水位与满状态可观测、满时阻塞不丢、取消不入队、消费后自动放行。

单测日志统一打印 `topic/partition`、`offset`、`delivery`、`committed` 以及
`criterion=...` 判定依据，便于人工核对每一步为何成立。
