package mq_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// killToDLQ 按 retryCfg 的确定性脚本，把 Poll 到的下一条消息连续失败到进死信。
// 每轮之间按退避推进可控时钟。返回进入死信时的死信记录。
func killToDLQ(t *testing.T, b *mq.Broker, c *mq.Consumer, clk *fakeClock, reason string) mq.DLQEntry {
	t.Helper()
	waits := []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}
	for round, w := range waits {
		if w > 0 {
			clk.advance(w)
		}
		ds, err := b.Poll(c, 8)
		if err != nil {
			t.Fatalf("killToDLQ poll: %v", err)
		}
		if len(ds) != 1 {
			t.Fatalf("killToDLQ round=%d 期望恰好 1 条投递，实际 %d", round, len(ds))
		}
		err = b.Nack(c, ds[0], reason)
		if round == 2 {
			if !errors.Is(err, mq.ErrRetryExhausted) {
				t.Fatalf("第 3 次失败应进死信，err=%v", err)
			}
		} else if err != nil {
			t.Fatalf("killToDLQ nack round=%d: %v", round, err)
		}
	}
	dlq := b.DeadLetters("g")
	if len(dlq) == 0 {
		t.Fatal("killToDLQ: 未产生死信")
	}
	return dlq[len(dlq)-1]
}

// pollOne 在给定 broker/消费者上拉取恰好一条投递，失败即终止测试。
func pollOne(t *testing.T, b *mq.Broker, c *mq.Consumer) mq.Delivery {
	t.Helper()
	ds, err := b.Poll(c, 8)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("期望恰好 1 条投递，实际 %d: %+v", len(ds), ds)
	}
	return ds[0]
}

// TestReplayFilterAndRedeliver 验证：按分区/位移/原因/时间范围筛选死信，
// 重投后作为新消息追加到同分区尾部，被同组正常消费，且携带完整溯源信息。
func TestReplayFilterAndRedeliver(t *testing.T) {
	clk := &fakeClock{t: time.Unix(9000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.LeaseTTL = time.Hour // 重投测试只关心可控时钟下的退避，避免租约过期干扰
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	if err := b.CreateTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	tp0 := mq.TopicPartition{Topic: "t", Partition: 0}
	tp1 := mq.TopicPartition{Topic: "t", Partition: 1}

	c, _ := b.JoinGroup("g", "c")

	// 分区0：两条不同失败原因的死信；分区1：一条死信。
	b.Produce(tp0, mq.Message{Value: []byte("p0-a")})
	deadA := killToDLQ(t, b, c, clk, "timeout")
	clk.advance(20 * time.Millisecond)

	b.Produce(tp0, mq.Message{Value: []byte("p0-b")})
	deadB := killToDLQ(t, b, c, clk, "bad-payload")
	clk.advance(20 * time.Millisecond)

	b.Produce(tp1, mq.Message{Value: []byte("p1-a")})
	deadP1 := killToDLQ(t, b, c, clk, "timeout")
	deadTimeP1 := b.DeadLetters("g")[2].DeadAt

	// 1) 按“原因包含 timeout”筛选：应只命中 p0-a 与 p1-a（不含 bad-payload）。
	res, err := b.ReplayDeadLetters("g", "key-by-reason",
		mq.ReplayFilter{ReasonContains: "timeout"})
	if err != nil {
		t.Fatalf("replay by reason: %v", err)
	}
	if res.Matched != 2 || res.EnqueuedNow != 2 {
		t.Fatalf("reason 筛选 matched=%d enqueued=%d want 2/2", res.Matched, res.EnqueuedNow)
	}
	if res.Operation.Status != mq.ReplayComplete || len(res.Operation.Items) != 2 {
		t.Fatalf("操作状态异常: %+v", res.Operation)
	}
	t.Logf("判定: 按原因 timeout 命中 2 条（%s,%s），bad-payload 未入选",
		"t-0#"+strconv.FormatInt(deadA.Offset, 10),
		"t-1#"+strconv.FormatInt(deadP1.Offset, 10))

	// 2) 按 分区+精确位移 只重投 p0-b。
	res2, err := b.ReplayDeadLetters("g", "key-by-tp-offset",
		mq.ReplayFilter{Topic: "t", Partitions: []int{0}, Offsets: []int64{deadB.Offset}})
	if err != nil {
		t.Fatalf("replay by tp/offset: %v", err)
	}
	if res2.Matched != 1 || res2.EnqueuedNow != 1 {
		t.Fatalf("tp/offset 筛选 matched=%d enqueued=%d want 1/1", res2.Matched, res2.EnqueuedNow)
	}
	t.Logf("判定: 按 分区0 + 位移%d 精确命中 p0-b", deadB.Offset)

	// 3) 时间范围筛选：只覆盖分区1死信时刻 -> 但它已被重投认领，
	//    默认 IncludeReplayed=false，应无候选。
	_, err = b.ReplayDeadLetters("g", "key-by-time",
		mq.ReplayFilter{DeadFrom: deadTimeP1.Add(-time.Millisecond), DeadTo: deadTimeP1.Add(time.Millisecond)})
	if !errors.Is(err, mq.ErrNothingToReplay) {
		t.Fatalf("已重投死信默认不应再入选，err=%v", err)
	}
	t.Log("判定: 已重投死信默认不再入选（防重复），IncludeReplayed=true 才可再次挑选")

	// 4) 重投消息按分区顺序追加在尾部：分区0 原有 2 条 -> 新位移 2、3；分区1 -> 新位移 1。
	batch, err := b.Poll(c, 8)
	if err != nil {
		t.Fatalf("poll replay batch: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("三条重投应同时可取，实际 %d: %+v", len(batch), batch)
	}
	wantOrder := []struct {
		part int
		off  int64
		val  string
	}{
		{0, 2, "p0-a"},
		{0, 3, "p0-b"},
		{1, 1, "p1-a"},
	}
	for i, want := range wantOrder {
		d := batch[i]
		if d.Record.Partition != want.part || d.Record.Offset != want.off ||
			string(d.Record.Value) != want.val || d.Attempt != 1 {
			t.Fatalf("重投第 %d 条异常: got %s val=%q attempt=%d want %d-%d/%s/attempt1",
				i, d.Record.MessageID(), string(d.Record.Value), d.Attempt,
				want.part, want.off, want.val)
		}
		if err := b.Ack(c, d); err != nil {
			t.Fatalf("ack %s: %v", d.Record.MessageID(), err)
		}
	}
	t.Log("判定: 三条重投按 (分区,位移) 顺序 t-0#2,t-0#3,t-1#1 投递，attempt 均重置为 1")

	// 第一条重投的溯源信息应完整保留原始失败次数、原因链与来源。
	got0 := batch[0]
	prov, ok := mq.ParseReplayProvenance(got0.Record.Headers)
	if !ok {
		t.Fatal("重投消息缺少溯源信息")
	}
	if prov.OriginTopic != "t" || prov.OriginPartition != 0 || prov.OriginOffset != deadA.Offset ||
		prov.OriginAttempts != 3 || prov.Round != 1 {
		t.Fatalf("溯源信息异常: %+v", prov)
	}
	if len(prov.OriginReasons) != 3 || prov.OriginReasons[0] != "timeout" {
		t.Fatalf("原始失败原因链未保留: %v", prov.OriginReasons)
	}
	t.Logf("判定: %s 携带溯源（源 %s-%d#%d, attempts=%d, round=%d, 原因链=%v）",
		got0.Record.MessageID(), prov.OriginTopic, prov.OriginPartition,
		prov.OriginOffset, prov.OriginAttempts, prov.Round, prov.OriginReasons)

	// 5) 全部确认后再 Poll 不应有重复投递；已提交位移不回退（分区0=4，分区1=2）。
	if more, _ := b.Poll(c, 8); len(more) != 0 {
		t.Fatalf("重投全部确认后仍有投递: %+v", more)
	}
	if err := b.CommitOffset(c, tp0, 4); err != nil {
		t.Fatalf("分区0提交位应为4: %v", err)
	}
	if err := b.CommitOffset(c, tp1, 2); err != nil {
		t.Fatalf("分区1提交位应为2: %v", err)
	}
	t.Log("判定: 重投消息确认后位移停在日志末尾，原死信位移未被重复投递")
}

// TestReplayIdempotent 验证：相同幂等键重复触发只恢复同一操作、不重复入队；
// 不同键再次触发同一死信默认被排除（除非显式 IncludeReplayed）。
func TestReplayIdempotent(t *testing.T) {
	clk := &fakeClock{t: time.Unix(11000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.LeaseTTL = time.Hour
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	c, _ := b.JoinGroup("g", "c")
	b.Produce(tp, mq.Message{Value: []byte("x")})
	dead := killToDLQ(t, b, c, clk, "boom")

	res1, err := b.ReplayDeadLetters("g", "same-key", mq.ReplayFilter{})
	if err != nil || res1.EnqueuedNow != 1 {
		t.Fatalf("首次重投异常: %+v err=%v", res1, err)
	}
	opID := res1.Operation.OpID

	// 相同键重复触发：返回同一操作，本次不再入队。
	res2, err := b.ReplayDeadLetters("g", "same-key", mq.ReplayFilter{})
	if err != nil {
		t.Fatalf("重复触发不应报错: %v", err)
	}
	if res2.Operation.OpID != opID || res2.EnqueuedNow != 0 {
		t.Fatalf("重复触发未幂等: op=%s(原 %s) enqueued=%d",
			res2.Operation.OpID, opID, res2.EnqueuedNow)
	}
	t.Logf("判定: 相同幂等键重复触发恢复操作 %s，新入队数=0", opID)

	// 不同键、默认筛选：该死信已被认领，ErrNothingToReplay。
	_, err = b.ReplayDeadLetters("g", "other-key", mq.ReplayFilter{})
	if !errors.Is(err, mq.ErrNothingToReplay) {
		t.Fatalf("已重投死信不应被另一操作重复入队，err=%v", err)
	}

	// GetReplay / ListReplays 可查询。
	got, err := b.GetReplay(opID)
	if err != nil || got.Status != mq.ReplayComplete {
		t.Fatalf("GetReplay 异常: %+v err=%v", got, err)
	}
	if ops := b.ListReplays("g"); len(ops) != 1 || ops[0].OpID != opID {
		t.Fatalf("ListReplays 异常: %+v", ops)
	}
	t.Logf("判定: 操作可查询 status=%s items=%d newOffset=%d",
		got.Status, len(got.Items), got.Items[0].NewOffset)

	// 日志中该源死信的重投副本只能有一条。
	d := pollOne(t, b, c)
	if d.Record.Offset != dead.Offset+1 || string(d.Record.Value) != "x" {
		t.Fatalf("重投副本异常: %+v", d)
	}
	if more, _ := b.Poll(c, 8); len(more) != 0 {
		t.Fatalf("存在重复重投副本: %+v", more)
	}
}

// TestReplayConcurrentIdempotent 验证多个调用方并发用同一幂等键触发，
// 只产生一个操作、一条重投副本。
func TestReplayConcurrentIdempotent(t *testing.T) {
	clk := &fakeClock{t: time.Unix(12000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.LeaseTTL = time.Hour
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	c, _ := b.JoinGroup("g", "c")
	b.Produce(tp, mq.Message{Value: []byte("x")})
	killToDLQ(t, b, c, clk, "boom")

	const N = 8
	var wg sync.WaitGroup
	opIDs := make([]string, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := b.ReplayDeadLetters("g", "concurrent-key", mq.ReplayFilter{})
			opIDs[i] = res.Operation.OpID
			errs[i] = err
		}(i)
	}
	wg.Wait()
	var first string
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Fatalf("并发重投 %d err=%v", i, errs[i])
		}
		if first == "" {
			first = opIDs[i]
		} else if opIDs[i] != first {
			t.Fatalf("并发触发产生了不同操作: %s vs %s", first, opIDs[i])
		}
	}
	if ops := b.ListReplays("g"); len(ops) != 1 {
		t.Fatalf("应只有 1 个重投操作，实际 %d", len(ops))
	}
	d := pollOne(t, b, c)
	if more, _ := b.Poll(c, 8); len(more) != 0 {
		t.Fatalf("并发触发产生重复副本: %+v", append([]mq.Delivery{d}, more...))
	}
	t.Log("判定: 8 个并发调用共享同一幂等键 -> 1 个操作、1 条重投副本")
}

// newReplayBroker 按给定（基于 t.TempDir 的）配置打开 broker 并建主题/入组。
func replayCfgFor(t *testing.T, clk *fakeClock, capacity int) mq.Config {
	cfg := retryCfg(t, clk)
	cfg.LeaseTTL = time.Hour
	cfg.PartCapacity = capacity
	return withLogger(cfg)
}

// TestReplayBackpressureAndResume 验证：分区容量不足时重投明确返回
// ErrQueueFull（不丢、不重复入队），操作处于 pending 且可查询；
// 容量恢复后 ResumeReplay 补齐；重启后用同一幂等键重试仍然不重复。
func TestReplayBackpressureAndResume(t *testing.T) {
	clk := &fakeClock{t: time.Unix(13000, 0)}
	cfg := replayCfgFor(t, clk, 5)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	b.CreateTopic("t", 1)
	c, _ := b.JoinGroup("g", "c")

	// 造 3 条死信（offset 0/1/2）。
	for i := 0; i < 3; i++ {
		clk.advance(20 * time.Millisecond)
		b.Produce(tp, mq.Message{Value: []byte("dead-" + strconv.Itoa(i))})
		killToDLQ(t, b, c, clk, "boom")
	}

	// 容量=5：重投依次占用 offset3、offset4，第 3 条装不下 -> ErrQueueFull。
	res, err := b.ReplayDeadLetters("g", "bp-key", mq.ReplayFilter{})
	if !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("容量满应返回 ErrQueueFull，实际 err=%v", err)
	}
	if res.EnqueuedNow != 2 {
		t.Fatalf("应已入队 2 条，实际 %d", res.EnqueuedNow)
	}
	if res.Operation.Status != mq.ReplayPending {
		t.Fatalf("未完成操作应为 pending，实际 %s", res.Operation.Status)
	}
	pending := res.Operation.Items[2]
	if pending.NewOffset != -1 {
		t.Fatalf("未入队条目不应占位移，NewOffset=%d", pending.NewOffset)
	}
	t.Logf("判定: 容量打满，2 条入队（offset3/4），第 3 条 ErrQueueFull，操作=%s 可查询",
		res.Operation.OpID)

	// 已入队的两条可以被正常消费；第三条绝没有提前混入。
	ds, _ := b.Poll(c, 8)
	if len(ds) != 2 || ds[0].Record.Offset != 3 || ds[1].Record.Offset != 4 {
		t.Fatalf("应只投递已入队的 2 条重投: %+v", ds)
	}
	for _, d := range ds {
		if err := b.Ack(c, d); err != nil {
			t.Fatal(err)
		}
	}

	// 立即 Resume：仍然装不下，继续反压（不能重复入队前两条）。
	res2, err := b.ResumeReplay(res.Operation.OpID)
	if !errors.Is(err, mq.ErrQueueFull) || res2.EnqueuedNow != 0 {
		t.Fatalf("容量未恢复时 Resume 应继续反压: err=%v enqueued=%d", err, res2.EnqueuedNow)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启且分区容量上调（运维扩容）：操作账本从磁盘恢复。
	cfg2 := cfg
	cfg2.PartCapacity = 8
	b2, err := mq.NewBroker(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	// 同一幂等键再次调用：恢复同一操作，续跑剩余 1 条。
	res3, err := b2.ReplayDeadLetters("g", "bp-key", mq.ReplayFilter{})
	if err != nil {
		t.Fatalf("重启后续跑失败: %v", err)
	}
	if res3.Operation.OpID != res.Operation.OpID || res3.EnqueuedNow != 1 {
		t.Fatalf("重启后应恢复同一操作并补齐 1 条: op=%s(原 %s) enqueued=%d",
			res3.Operation.OpID, res.Operation.OpID, res3.EnqueuedNow)
	}
	if res3.Operation.Status != mq.ReplayComplete {
		t.Fatalf("操作应完成，实际 %s", res3.Operation.Status)
	}
	// 前两条的状态也应完整恢复（NewOffset=3/4）。
	if res3.Operation.Items[0].NewOffset != 3 || res3.Operation.Items[1].NewOffset != 4 {
		t.Fatalf("已完成条目状态恢复异常: %+v", res3.Operation.Items)
	}
	t.Log("判定: 重启后同幂等键恢复 pending 操作并续跑，已完成条目不重复入队")

	// 再用同键调用一次：幂等，无新增。
	res4, err := b2.ReplayDeadLetters("g", "bp-key", mq.ReplayFilter{})
	if err != nil || res4.EnqueuedNow != 0 {
		t.Fatalf("完成后重复调用应幂等成功: err=%v enqueued=%d", err, res4.EnqueuedNow)
	}
	c2, _ := b2.JoinGroup("g", "c2")
	rest, _ := b2.Poll(c2, 8)
	if len(rest) != 1 || rest[0].Record.Offset != 5 || string(rest[0].Record.Value) != "dead-2" {
		t.Fatalf("重启后只应补投第 3 条重投(offset5)，实际: %+v", rest)
	}
	prov, _ := mq.ParseReplayProvenance(rest[0].Record.Headers)
	if prov.OriginOffset != 2 {
		t.Fatalf("溯源源位移应为2，实际 %+v", prov)
	}
	t.Log("判定: 反压补齐的消息内容与溯源完整，且全流程无重复无丢失")
}

// TestReplayRestartRecoversCompleteOp 验证：已完成的重投操作在重启后
// 仍可查询，且死信的认领标记持久化（不会被新操作再次挑选）。
func TestReplayRestartRecoversCompleteOp(t *testing.T) {
	clk := &fakeClock{t: time.Unix(14000, 0)}
	cfg := replayCfgFor(t, clk, 10)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	b.CreateTopic("t", 1)
	c, _ := b.JoinGroup("g", "c")
	b.Produce(tp, mq.Message{Value: []byte("v")})
	killToDLQ(t, b, c, clk, "boom")
	res, err := b.ReplayDeadLetters("g", "restart-key", mq.ReplayFilter{})
	if err != nil {
		t.Fatal(err)
	}
	opID := res.Operation.OpID
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, _ := mq.NewBroker(cfg)
	defer b2.Close()
	b2.CreateTopic("t", 1)
	got, err := b2.GetReplay(opID)
	if err != nil || got.Status != mq.ReplayComplete || got.Items[0].NewOffset != 1 {
		t.Fatalf("重启后操作恢复异常: %+v err=%v", got, err)
	}
	// 同键恢复：无新增；默认筛选下别的键也挑不到这条已认领死信。
	r2, err := b2.ReplayDeadLetters("g", "restart-key", mq.ReplayFilter{})
	if err != nil || r2.EnqueuedNow != 0 {
		t.Fatalf("重启后同键应幂等: err=%v enqueued=%d", err, r2.EnqueuedNow)
	}
	if _, err := b2.ReplayDeadLetters("g", "new-key", mq.ReplayFilter{}); !errors.Is(err, mq.ErrNothingToReplay) {
		t.Fatalf("已认领死信不应再被挑选，err=%v", err)
	}
	t.Log("判定: 已完成操作及其认领标记在重启后完整恢复、可查询、不可重复入队")
}

// TestReplayFailAgain 验证：重投消息重新获得完整的确定性退避重试机会；
// 若再次超过上限，会作为【新死信记录】再次进死信（携带新一轮原因链），
// 且原死信的溯源信息仍在；这条新死信还能被再次重投（round 递增）。
func TestReplayFailAgain(t *testing.T) {
	clk := &fakeClock{t: time.Unix(15000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.LeaseTTL = time.Hour
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	c, _ := b.JoinGroup("g", "c")

	b.Produce(tp, mq.Message{Value: []byte("poison")})
	first := killToDLQ(t, b, c, clk, "original-boom")
	if first.Offset != 0 {
		t.Fatalf("原死信位移=%d want 0", first.Offset)
	}

	res, err := b.ReplayDeadLetters("g", "fail-again", mq.ReplayFilter{})
	if err != nil {
		t.Fatal(err)
	}
	opID := res.Operation.OpID

	// 重投副本在 offset1，再次按确定性脚本失败 3 次 -> 再次进死信。
	second := killToDLQ(t, b, c, clk, "replayed-boom")
	if second.Offset != 1 {
		t.Fatalf("新死信应对应重投副本位移1，实际 %d", second.Offset)
	}
	if second.Attempts != 3 || len(second.Reasons) != 3 || second.Reasons[0] != "replayed-boom" {
		t.Fatalf("新死信原因链异常: attempts=%d reasons=%v", second.Attempts, second.Reasons)
	}
	prov, ok := mq.ParseReplayProvenance(second.Record.Headers)
	if !ok || prov.OriginOffset != 0 || prov.OriginAttempts != 3 {
		t.Fatalf("新死信应保留对原死信的溯源: %+v ok=%v", prov, ok)
	}
	dlq := b.DeadLetters("g")
	if len(dlq) != 2 {
		t.Fatalf("原死信与新死信应并存，共 2 条，实际 %d", len(dlq))
	}
	t.Logf("判定: 重投副本 %s 再次超限 -> 新死信（新一轮 attempts=3 原因链=%v），原死信 %s#0 保留",
		second.Record.MessageID(), second.Reasons, "t")

	// 新死信可被再次重投：IncludeReplayed=true 才能选中它（它头上带重投溯源，
	// 但 DLQEntry.ReplayedOpID 对原死信已设置；对 offset1 这条新死信尚未被重投）。
	// 用精确位移挑选 offset1，默认筛选即可命中（这条死信本身还没被重投过）。
	res2, err := b.ReplayDeadLetters("g", "second-replay",
		mq.ReplayFilter{Offsets: []int64{1}})
	if err != nil {
		t.Fatalf("再次重投: %v", err)
	}
	if res2.EnqueuedNow != 1 {
		t.Fatalf("应重投 1 条，实际 %d", res2.EnqueuedNow)
	}
	d := pollOne(t, b, c)
	if d.Record.Offset != 2 {
		t.Fatalf("二次重投副本应在 offset2，实际 %s", d.Record.MessageID())
	}
	prov2, _ := mq.ParseReplayProvenance(d.Record.Headers)
	if prov2.Round != 2 || prov2.OriginOffset != 0 || prov2.OpID == opID {
		t.Fatalf("二次重投溯源异常: %+v（round 应=2，源仍指向 offset0，opID 应为新操作）", prov2)
	}
	if err := b.Ack(c, d); err != nil {
		t.Fatal(err)
	}
	t.Logf("判定: 新死信可再次重投为 %s，round=2，溯源仍指向最初来源 t-0#0",
		d.Record.MessageID())
}

// TestReplayOrderingRegression 验证重投不破坏顺序与位移：
// 已确认提交的消息不会因重投被再投；重投消息排在同分区所有现存消息之后；
// 同分区同一时刻仍只有一个持有者能处理（沿用独占与租约）。
func TestReplayOrderingRegression(t *testing.T) {
	clk := &fakeClock{t: time.Unix(16000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.LeaseTTL = time.Hour
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	c1, _ := b.JoinGroup("g", "owner")

	// offset0 打进死信；offset1 正常消费并提交。
	b.Produce(tp, mq.Message{Value: []byte("dead")})
	killToDLQ(t, b, c1, clk, "boom")
	b.Produce(tp, mq.Message{Value: []byte("good")})
	d := pollOne(t, b, c1)
	if d.Record.Offset != 1 {
		t.Fatalf("正常消息位移异常: %s", d.Record.MessageID())
	}
	if err := b.Ack(c1, d); err != nil {
		t.Fatal(err)
	}
	if err := b.CommitOffset(c1, tp, 2); err != nil {
		t.Fatalf("提交位推进到2: %v", err)
	}

	// 重投死信：只能落在 offset2，绝不能复活 offset0/offset1。
	if _, err := b.ReplayDeadLetters("g", "order-key", mq.ReplayFilter{}); err != nil {
		t.Fatal(err)
	}
	rd := pollOne(t, b, c1)
	if rd.Record.Offset != 2 || string(rd.Record.Value) != "dead" {
		t.Fatalf("重投副本异常: %+v", rd)
	}
	t.Logf("判定: 已提交到 offset2 后重投，新副本是 %s，offset0/1 未被再投", rd.Record.MessageID())

	// 非持有者 c2 不能确认这条在途重投（租约/独占栅栏不回归）。
	// 同组只有 c1 持有分区，后加入的 c2 不会在粘滞分配下抢到它。
	c2, _ := b.JoinGroup("g", "other")
	if err := b.Ack(c2, rd); err == nil {
		t.Fatal("非持有者竟然确认了重投消息")
	} else {
		t.Logf("判定: 非持有者确认重投被拒绝（%v），同分区独占语义保持", err)
	}
	if err := b.Ack(c1, rd); err != nil {
		t.Fatalf("持有者确认失败: %v", err)
	}
	if more, _ := b.Poll(c1, 8); len(more) != 0 {
		t.Fatalf("确认后不应再有投递: %+v", more)
	}
}

// TestReplayInflightBackpressure 验证重投入队后的消费同样受 MaxInflight 约束：
// 窗口被未确认消息占满时，其余重投消息排队等待，不丢不重。
func TestReplayInflightBackpressure(t *testing.T) {
	clk := &fakeClock{t: time.Unix(17000, 0)}
	cfg := replayCfgFor(t, clk, 20)
	cfg.MaxInflight = 2
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	c, _ := b.JoinGroup("g", "c")
	for i := 0; i < 3; i++ {
		clk.advance(20 * time.Millisecond)
		b.Produce(tp, mq.Message{Value: []byte("d" + strconv.Itoa(i))})
		killToDLQ(t, b, c, clk, "boom")
	}
	res, err := b.ReplayDeadLetters("g", "inflight-key", mq.ReplayFilter{})
	if err != nil || res.EnqueuedNow != 3 {
		t.Fatalf("重投 3 条: err=%v enqueued=%d", err, res.EnqueuedNow)
	}
	first, _ := b.Poll(c, 8)
	if len(first) != cfg.MaxInflight {
		t.Fatalf("在途窗口=%d 应只投递 2 条，实际 %d", cfg.MaxInflight, len(first))
	}
	// 确认一条后，下一次 Poll 才放出第 3 条。
	if err := b.Ack(c, first[0]); err != nil {
		t.Fatal(err)
	}
	second, _ := b.Poll(c, 8)
	if len(second) != 1 || second[0].Record.Offset != 5 {
		t.Fatalf("窗口释放后应放出 offset5，实际: %+v", second)
	}
	t.Log("判定: 重投消息消费受 MaxInflight=2 反压，逐条放行，不丢不重")
}
