# Durable Message Queue (Go, local-disk)

零外部依赖的本地持久化消息队列与消费组实现，用本地磁盘模拟消息存储与位移管理。
支持分区有序、消费组并发与分区独占、确定性失败重试与死信、进程重启恢复。

## 特性

- **分区有序**：同 Key 稳定路由到同一分区，分区内 offset 单调、顺序稳定。
- **持久化**：CRC32 保护的追加日志 + 原子 JSON 状态文件；重启后未确认消息重投。
- **位移幂等**：重复提交是 no-op；回退返回 `offset_regress`，越界返回 `offset_out_of_range`。
- **消费组独占**：确定性轮询分配 + 世代(generation) + 租约；过期分区安全再平衡。
- **重试与死信**：无随机抖动的确定性退避（恒定/线性），超上限进死信并保留原始原因。
- **背压可观测**：容量按最慢消费组积压计算，满时阻塞不丢消息，水位/lag/in-flight 可查。

## 快速上手

```go
b, _ := mq.Open(mq.BrokerConfig{
    RootDir: "./data", Partitions: 2,
    PartitionCap: 1000, MaxRetries: 3,
    LeaseTTL: 10 * time.Second,
    Backoff:  mq.LinearBackoff{Base: 100 * time.Millisecond},
})
defer b.Close()
b.CreateTopic("orders")

// 生产
tp, off, _ := b.Produce(ctx, "orders", mq.Message{Key: "user-1", Body: payload})

// 消费组
gen, tps, _ := b.JoinGroup("billing", "worker-1")
for {
    d, err := b.Fetch(ctx, "billing", "worker-1", gen, tps[0])
    if err != nil { /* 处理 ErrStaleGeneration 后重新 JoinGroup */ }
    if processErr != nil {
        b.Nack("billing", "worker-1", gen, d, processErr.Error()) // 退避重试 / 死信
    } else {
        b.Ack("billing", "worker-1", gen, d)                      // 提交 offset+1
    }
}
```

## 处理再平衡

`JoinGroup/Heartbeat` 返回当前世代号；当 `Fetch/Ack/Nack` 返回
`mq.ErrStaleGeneration` 或 `mq.ErrPartitionNotOwned` 时，说明分区已被重新分配，
消费者应重新 `JoinGroup` 获取新世代与新分区集合。消费者存活期间应周期性
`Heartbeat`（间隔 < LeaseTTL）。

## 验证

```bash
go test ./... -v       # 单测日志含消息标识/offset/committed/criterion
go test ./... -race    # 并发安全
```

完整语义（消息模型、消费组与位移、失败与死信规则、容量水位、错误码、测试清单）
见 [docs/design.md](docs/design.md)。
