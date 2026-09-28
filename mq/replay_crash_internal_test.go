package mq

import (
	"path/filepath"
	"testing"
	"time"
)

// TestReplayCrashWindowsInternal 直接在持久化层模拟两类崩溃窗口：
//  1. 账本已落盘、部分死信认领标记未落盘就崩溃 -> 重启后同键恢复，
//     未认领条目应被重新认领且只入队一次；
//  2. 重投消息已写入分区日志，但“已入队”标记未完成就崩溃 ->
//     重启后续跑时通过日志尾部探测认回，绝不追加第二条副本。
func TestReplayCrashWindowsInternal(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(7000, 0)
	clk := &fixedClock{t: now}
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 50 * time.Millisecond, Multiplier: 2}

	// 准备：造出两条死信。
	cfg := Config{
		DataDir: filepath.Join(dir, "data"), LeaseTTL: time.Hour,
		PartCapacity: 10, MaxInflight: 4, Retry: policy, Clock: clk,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	tp := TopicPartition{Topic: "t", Partition: 0}
	for i := 0; i < 2; i++ {
		if _, err := b.Produce(tp, Message{Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	rec0, ok := b.ReadRecord(tp, 0)
	if !ok {
		t.Fatal("rec0")
	}
	rec1, ok := b.ReadRecord(tp, 1)
	if !ok {
		t.Fatal("rec1")
	}
	// 直接在 retryStore 层写入两条死信快照（绕过消费脚本）。
	if err := b.retries.schedule(retryEntry{
		Group: "g", Topic: "t", Partition: 0, Offset: 0,
		Attempt: 3, LastReason: "boom-0",
	}, rec0, policy, now); err != ErrRetryExhausted {
		t.Fatalf("dead0 err=%v", err)
	}
	if err := b.retries.schedule(retryEntry{
		Group: "g", Topic: "t", Partition: 0, Offset: 1,
		Attempt: 3, LastReason: "boom-1",
	}, rec1, policy, now.Add(time.Millisecond)); err != ErrRetryExhausted {
		t.Fatalf("dead1 err=%v", err)
	}

	// 窗口1：正常创建操作（账本先落盘），但只认领第一条，模拟第二条认领前崩溃。
	candidates := selectReplayCandidates(b.retries.deadLetters("g"), ReplayFilter{})
	if len(candidates) != 2 {
		t.Fatalf("候选=%d want 2", len(candidates))
	}
	op, _ := b.replays.createReplay(b.retries, "g", "crash-key", candidates, now, 1)
	opID := op.OpID
	if len(op.Items) != 2 {
		t.Fatalf("操作条目=%d want 2", len(op.Items))
	}
	// 取消第二条的认领标记，模拟“账本已落盘、第二条认领尚未发生”。
	b.retries.mu.Lock()
	for i := range b.retries.dead["g"] {
		e := &b.retries.dead["g"][i]
		if e.Offset == 1 {
			e.ReplayedOpID = ""
			e.ReplayedAt = time.Time{}
			e.ReplayedNewOffset = 0
		}
	}
	_ = b.retries.persistLocked()
	b.retries.mu.Unlock()

	// 窗口2：直接把第一条的重投副本追加进日志，但不标记“已入队”，
	// 模拟 Append+fsync 已完成、状态回写前崩溃。
	msg := buildReplayMessage(opID,
		DLQEntry{Group: "g", Topic: "t", Partition: 0, Offset: 0,
			Record: rec0, Attempts: 3, Reasons: []string{"boom-0"}, DeadAt: now}, now)
	written, err := b.logs[tp].Append(msg)
	if err != nil {
		t.Fatal(err)
	}
	if written.Offset != 2 {
		t.Fatalf("手写重投副本位移=%d want 2", written.Offset)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：用同一幂等键恢复。
	b2, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	res, err := b2.ReplayDeadLetters("g", "crash-key", ReplayFilter{})
	if err != nil {
		t.Fatalf("崩溃恢复重投失败: %v", err)
	}
	if res.Operation.OpID != opID {
		t.Fatalf("应恢复同一操作: got %s want %s", res.Operation.OpID, opID)
	}
	if res.Operation.Status != ReplayComplete {
		t.Fatalf("恢复后应完成，实际 %s items=%+v", res.Operation.Status, res.Operation.Items)
	}
	// 第一条认回已存在副本（already），第二条真正新入队；两条都只对应一条副本。
	if res.EnqueuedNow != 1 || res.AlreadyDone != 1 {
		t.Fatalf("enqueued=%d already=%d want 1/1", res.EnqueuedNow, res.AlreadyDone)
	}
	newoff := map[int64]int64{}
	for _, it := range res.Operation.Items {
		newoff[it.OriginOffset] = it.NewOffset
	}
	if newoff[0] != 2 || newoff[1] != 3 {
		t.Fatalf("新位移映射异常: %+v（源0应认回2，源1应新入队3）", newoff)
	}
	// 分区日志此刻长度应为 4，且只有 offset2/3 两条重投副本，无重复追加。
	l := b2.logs[tp]
	if l.endOffset() != 4 {
		t.Fatalf("日志长度=%d want 4", l.endOffset())
	}
	for _, off := range []int64{2, 3} {
		r, ok := l.Read(off)
		if !ok {
			t.Fatalf("读取 %d 失败", off)
		}
		prov, isReplay := ParseReplayProvenance(r.Headers)
		if !isReplay || prov.OpID != opID {
			t.Fatalf("offset %d 重投溯源异常: %+v", off, r.Headers)
		}
	}
	// 同键再来一次：幂等，无新增。
	res2, err := b2.ReplayDeadLetters("g", "crash-key", ReplayFilter{})
	if err != nil || res2.EnqueuedNow != 0 || res2.AlreadyDone != 2 {
		t.Fatalf("恢复完成后重复调用应幂等: err=%v enqueued=%d already=%d",
			err, res2.EnqueuedNow, res2.AlreadyDone)
	}
}

// fixedClock 是内部测试用的简单可控时钟。
type fixedClock struct{ t time.Time }

func (c *fixedClock) Now() time.Time { return c.t }
