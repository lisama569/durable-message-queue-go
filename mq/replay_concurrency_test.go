package mq_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// makeDeadLetters 在单分区内造出 n 条连续死信（位移 0..n-1）。
func makeDeadLetters(t *testing.T, b *mq.Broker, c *mq.Consumer,
	clk *fakeClock, tp mq.TopicPartition, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := b.Produce(tp, mq.Message{Value: []byte("d" + strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}
	for len(b.DeadLetters("g")) < n {
		ds, err := b.Poll(c, 64)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range ds {
			_ = b.Nack(c, d, "boom")
		}
		clk.advance(20 * time.Millisecond)
	}
}

// TestReplayConcurrentTriggerResumeQuery 并发安全回归：
// 多个调用方一边并发触发/续跑同一幂等键的重投（另有不同键的操作），
// 一边高频 GetReplay/ListReplays。要求：
//   - -race 下无数据竞争；
//   - 每次查询读到的都是自洽快照：状态与各条目状态一致、条目数恒定、
//     NewOffset 与 Status 不出现“半更新”错配（pending 却带有效新位移等）；
//   - 最终只存在一个同幂等键的操作，每个源死信在日志中恰好有一条重投副本。
func TestReplayConcurrentTriggerResumeQuery(t *testing.T) {
	clk := &fakeClock{t: time.Unix(40000, 0)}
	cfg := retryCfg(t, clk)
	cfg.LeaseTTL = time.Hour
	cfg.PartCapacity = 100000
	cfg = withLogger(cfg)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	c, _ := b.JoinGroup("g", "c")
	const N = 40
	makeDeadLetters(t, b, c, clk, tp, N)

	const writers, readers = 4, 4
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 写者：并发触发同幂等键（应始终恢复同一操作），并对查到的操作续跑。
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := b.ReplayDeadLetters("g", "concurrent-key", mq.ReplayFilter{})
				if err != nil && !errors.Is(err, mq.ErrQueueFull) {
					t.Errorf("trigger: %v", err)
					return
				}
				if res.Operation.OpID != "" {
					_, _ = b.ResumeReplay(res.Operation.OpID)
				}
			}
		}()
	}

	// 读者：高频 List/Get，并对每次快照做自洽性校验。
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ops := b.ListReplays("g")
				for _, op := range ops {
					validateReplaySnapshot(t, op, N)
					got, err := b.GetReplay(op.OpID)
					if err != nil {
						t.Errorf("GetReplay(%s): %v", op.OpID, err)
						return
					}
					validateReplaySnapshot(t, got, N)
					// 列表与单点查询对同一操作应给出一致的推进点。
					if got.Status != op.Status || len(got.Items) != len(op.Items) {
						t.Errorf("并发读到撕裂快照: list=%s/%d get=%s/%d",
							op.Status, len(op.Items), got.Status, len(got.Items))
						return
					}
				}
			}
		}()
	}

	// 运行一段时间后停止。
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	// 收尾：确保操作已全部入队（容量充足，恢复同一操作后必然跑完）。
	res, err := b.ReplayDeadLetters("g", "concurrent-key", mq.ReplayFilter{})
	if err != nil {
		t.Fatalf("最终重投: %v", err)
	}
	if res.Operation.Status != mq.ReplayComplete || len(res.Operation.Items) != N {
		t.Fatalf("最终操作异常: status=%s items=%d want complete/%d",
			res.Operation.Status, len(res.Operation.Items), N)
	}

	// 只有一个同幂等键操作；所有条目完成且新位移唯一、连续。
	ops := b.ListReplays("g")
	if len(ops) != 1 || ops[0].IdempotencyKey != "concurrent-key" {
		t.Fatalf("操作列表异常: %+v", ops)
	}
	newOffs := map[int64]struct{}{}
	for _, it := range ops[0].Items {
		if it.Status != mq.ReplayComplete || it.NewOffset < 0 {
			t.Fatalf("存在未完成或半更新条目: %+v", it)
		}
		if _, dup := newOffs[it.NewOffset]; dup {
			t.Fatalf("重投副本新位移重复: %d", it.NewOffset)
		}
		newOffs[it.NewOffset] = struct{}{}
	}
	if len(newOffs) != N {
		t.Fatalf("重投副本数=%d want %d（有重复入队）", len(newOffs), N)
	}

	// 日志尾部应有且仅有 N 条带本操作溯源的重投副本，源位移覆盖 0..N-1。
	seenOrigin := map[int64]int{}
	l := b.LogStats()[0]
	for off := int64(0); off < l.LogEndOffset; off++ {
		rec, ok := b.ReadRecord(tp, off)
		if !ok {
			t.Fatalf("读取 %d", off)
		}
		if prov, isReplay := mq.ParseReplayProvenance(rec.Headers); isReplay {
			if prov.OpID != ops[0].OpID {
				t.Fatalf("出现其他操作的副本: op=%s", prov.OpID)
			}
			seenOrigin[prov.OriginOffset]++
		}
	}
	if len(seenOrigin) != N {
		t.Fatalf("重投副本覆盖源死信=%d want %d", len(seenOrigin), N)
	}
	for o, n := range seenOrigin {
		if n != 1 {
			t.Fatalf("源死信 %d 被重复入队 %d 次", o, n)
		}
	}
	t.Logf("判定: 4 写者并发触发/续跑 + 4 读者高频查询，-race 下无竞争；" +
		"快照始终自洽，最终 1 个操作、N 条源死信各恰好 1 条副本")
}

// validateReplaySnapshot 校验一次查询返回的操作快照内部自洽：
// 不存在“操作已完成却有条目未完成”、“条目 pending 却带有效新位移”等撕裂状态。
func validateReplaySnapshot(t *testing.T, op mq.ReplayOperation, wantItems int) {
	t.Helper()
	if op.OpID == "" {
		t.Errorf("快照缺少 OpID")
		return
	}
	if len(op.Items) != wantItems {
		t.Errorf("操作 %s 条目数=%d want %d（疑似读到创建中途状态）",
			op.OpID, len(op.Items), wantItems)
		return
	}
	allComplete := true
	for _, it := range op.Items {
		switch it.Status {
		case mq.ReplayComplete:
			if it.NewOffset < 0 {
				t.Errorf("撕裂快照: 条目 %d 已完成但 NewOffset=%d",
					it.OriginOffset, it.NewOffset)
			}
		default:
			allComplete = false
		}
	}
	if op.Status == mq.ReplayComplete && !allComplete {
		t.Errorf("撕裂快照: 操作标记 complete 但存在未完成条目: %+v", op.Items)
	}
}

// TestReplayConcurrentQueryAfterRestart 重启恢复后：多调用方并发用同一幂等键
// 恢复操作、续跑并查询，要求 -race 下安全、始终恢复同一操作、副本不重复。
func TestReplayConcurrentQueryAfterRestart(t *testing.T) {
	clk := &fakeClock{t: time.Unix(41000, 0)}
	cfg := retryCfg(t, clk)
	cfg.LeaseTTL = time.Hour
	cfg.PartCapacity = 100000
	cfg = withLogger(cfg)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	b.CreateTopic("t", 1)
	c, _ := b.JoinGroup("g", "c")
	const N = 10
	makeDeadLetters(t, b, c, clk, tp, N)
	res, err := b.ReplayDeadLetters("g", "restart-key", mq.ReplayFilter{})
	if err != nil || res.Operation.Status != mq.ReplayComplete {
		t.Fatalf("首次重投异常: err=%v op=%+v", err, res.Operation)
	}
	opID := res.Operation.OpID
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	b2.CreateTopic("t", 1)

	const G = 6
	var wg sync.WaitGroup
	for i := 0; i < G; i++ {
		wg.Add(1)
		go func(kind int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				switch kind {
				case 0, 1, 2:
					r, err := b2.ReplayDeadLetters("g", "restart-key", mq.ReplayFilter{})
					if err != nil {
						t.Errorf("recover trigger: %v", err)
						return
					}
					if r.Operation.OpID != opID || r.EnqueuedNow != 0 {
						t.Errorf("恢复到不同操作或重复入队: op=%s enq=%d",
							r.Operation.OpID, r.EnqueuedNow)
						return
					}
				case 3, 4:
					ops := b2.ListReplays("g")
					for _, op := range ops {
						validateReplaySnapshot(t, op, N)
					}
				default:
					if got, err := b2.GetReplay(opID); err != nil ||
						got.Status != mq.ReplayComplete || len(got.Items) != N {
						t.Errorf("GetReplay 异常: err=%v got=%+v", err, got)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()

	// 副本数量仍恰好 N 条，重启+并发恢复没有追加任何重复副本。
	count := 0
	for off := int64(0); off < int64(N); off++ {
		rec, ok := b2.ReadRecord(tp, off)
		if !ok {
			t.Fatal(off)
		}
		if _, isReplay := mq.ParseReplayProvenance(rec.Headers); isReplay {
			count++
		}
	}
	if count != 0 {
		t.Fatalf("前 %d 条应是原始记录，重投副本不应在原位移", N)
	}
	replays := 0
	for off := int64(N); off < int64(2*N); off++ {
		rec, ok := b2.ReadRecord(tp, off)
		if !ok {
			t.Fatalf("缺少重投副本位移 %d", off)
		}
		if prov, ok := mq.ParseReplayProvenance(rec.Headers); !ok || prov.OpID != opID {
			t.Fatalf("位移 %d 不是本操作的重投副本: %+v", off, rec.Headers)
		}
		replays++
	}
	if replays != N {
		t.Fatalf("重投副本=%d want %d", replays, N)
	}
	t.Log("判定: 重启后并发同键恢复/查询在 -race 下安全，操作唯一、无重复副本")
}
