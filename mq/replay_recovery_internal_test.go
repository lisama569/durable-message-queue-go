package mq

import (
	"testing"
	"time"
)

// newReplayBroker 构造内部可控 Broker 便于直接操作存储层。
func newReplayBroker(t *testing.T, capacity int) *Broker {
	t.Helper()
	clk := &testClock{t: time.Unix(70000, 0)}
	b, err := NewBroker(Config{
		DataDir:      t.TempDir(),
		LeaseTTL:     time.Hour,
		PartCapacity: capacity,
		MaxInflight:  4,
		Retry:        DefaultRetryPolicy(),
		Clock:        clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

type testClock struct{ t time.Time }

func (c *testClock) Now() time.Time { return c.t }

// seedDead 直接写入若干死信记录（绕过消费流程），返回对应日志记录。
func seedDead(t *testing.T, b *Broker, tp TopicPartition, n int, reason string) {
	t.Helper()
	if err := b.CreateTopic(tp.Topic, tp.Partition+1); err != nil {
		t.Fatal(err)
	}
	entries := make([]DLQEntry, 0, n)
	for i := 0; i < n; i++ {
		rec, err := b.logs[tp].Append(Message{Value: []byte("p")})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, DLQEntry{
			Group: "g", Topic: tp.Topic, Partition: tp.Partition, Offset: rec.Offset,
			Record: rec, Attempts: 3, Reasons: []string{reason},
			DeadAt: b.clock.Now(),
		})
	}
	b.retries.mu.Lock()
	b.retries.dead["g"] = append(b.retries.dead["g"], entries...)
	if err := b.retries.persistLocked(); err != nil {
		t.Fatal(err)
	}
	b.retries.mu.Unlock()
}

// TestReplayCrashReconcile 模拟“请求已落盘、部分条目已追加到日志，但进度未回写”
// 的崩溃窗口：恢复时必须对账出已落盘条目（不重复追加），补齐其余条目。
func TestReplayCrashReconcile(t *testing.T) {
	b := newReplayBroker(t, 100)
	tp := TopicPartition{Topic: "t", Partition: 0}
	seedDead(t, b, tp, 4, "boom")

	// 构造并落盘一个 pending 请求（4 条目，同分区 Base=4）。
	now := b.clock.Now()
	req := replayRequest{
		RequestID: "rp-crash-1", Group: "g", CreatedAt: now, Status: ReplayPending,
	}
	for i := int64(0); i < 4; i++ {
		req.Items = append(req.Items, replayItemState{
			Topic: "t", Partition: 0, OldOffset: i, Base: 4, NewOffset: -1,
		})
	}
	if err := b.replays.create(req); err != nil {
		t.Fatal(err)
	}
	// 标记前 2 条死信“已被认领”（与崩溃点一致），并直接把对应新记录追加到日志，
	// 但请求进度完全不更新——模拟 fsync 之后、persist 之前崩溃。
	dead := b.retries.deadLetters("g")
	if len(dead) != 4 {
		t.Fatalf("seed dead=%d", len(dead))
	}
	for i := 0; i < 2; i++ {
		o := ReplayOrigin{
			RequestID: req.RequestID, Group: "g", Topic: "t", Partition: 0,
			Offset: int64(i), Attempts: 3, Reasons: dead[i].Reasons,
			DeadAt: dead[i].DeadAt, FirstEnqueuedAt: dead[i].Record.EnqueuedAt, Hop: 1,
		}
		msg := Message{
			Key:     append([]byte(nil), dead[i].Record.Key...),
			Value:   append([]byte(nil), dead[i].Record.Value...),
			Headers: encodeOriginHeaders(dead[i].Record.Headers, o),
		}
		if _, err := b.logs[tp].Append(msg); err != nil {
			t.Fatal(err)
		}
	}
	if got := b.logs[tp].len(); got != 6 {
		t.Fatalf("崩溃点日志长度=%d want 6", got)
	}

	// 恢复：不得重复追加前 2 条，只需补齐后 2 条。
	got, ok := b.replays.get("g", req.RequestID)
	if !ok {
		t.Fatal("重投请求未落盘")
	}
	b.mu.Lock()
	resumed, rerr := b.runReplayLocked(got, b.clock.Now())
	b.mu.Unlock()
	if rerr != nil {
		t.Fatalf("恢复执行失败: %v", rerr)
	}
	if resumed.Status != ReplayCompleted {
		t.Fatalf("恢复后状态=%s want completed", resumed.Status)
	}
	for i, it := range resumed.Items {
		if !it.Appended || it.NewOffset != int64(4+i) {
			t.Fatalf("条目 %d 恢复异常: %+v", i, it)
		}
	}
	if got := b.logs[tp].len(); got != 8 {
		t.Fatalf("恢复后日志长度=%d want 8（已落盘条目不得重复追加）", got)
	}
	// 日志中恰好 4 条带本请求 ID 的重投记录，且新位移连续 4..7。
	count := 0
	for off := int64(4); off < 8; off++ {
		rec, ok := b.logs[tp].Read(off)
		if !ok {
			t.Fatalf("读取 off=%d 失败", off)
		}
		o := originFromHeaders(rec.Headers)
		if o == nil || o.RequestID != req.RequestID {
			t.Fatalf("off=%d 来源头异常: %+v", off, o)
		}
		count++
	}
	if count != 4 {
		t.Fatalf("重投记录=%d want 4（不重不漏）", count)
	}
	// 4 条死信均被标记已重投，且指向同一个请求 ID。
	for _, d := range b.retries.deadLetters("g") {
		if !d.Replayed || d.ReplayID != req.RequestID {
			t.Fatalf("死信认领标记异常: %+v", d)
		}
	}
}

// TestReplayInterleavedProduceReconcile 模拟崩溃点后、恢复前有并发 Produce 在
// Base 之后插入了普通消息：对账仍应依据重投头识别本请求条目，不得误判。
func TestReplayInterleavedProduceReconcile(t *testing.T) {
	b := newReplayBroker(t, 100)
	tp := TopicPartition{Topic: "t", Partition: 0}
	seedDead(t, b, tp, 2, "boom")

	req := replayRequest{
		RequestID: "rp-crash-2", Group: "g", CreatedAt: b.clock.Now(), Status: ReplayPending,
	}
	for i := int64(0); i < 2; i++ {
		req.Items = append(req.Items, replayItemState{
			Topic: "t", Partition: 0, OldOffset: i, Base: 2, NewOffset: -1,
		})
	}
	if err := b.replays.create(req); err != nil {
		t.Fatal(err)
	}
	// 崩溃前只追加了第 1 条重投（off=2），随后有普通生产插入（off=3）。
	dead := b.retries.deadLetters("g")
	o := ReplayOrigin{
		RequestID: req.RequestID, Group: "g", Topic: "t", Partition: 0,
		Offset: 0, Attempts: 3, Reasons: dead[0].Reasons,
		DeadAt: dead[0].DeadAt, FirstEnqueuedAt: dead[0].Record.EnqueuedAt, Hop: 1,
	}
	if _, err := b.logs[tp].Append(Message{
		Value:   append([]byte(nil), dead[0].Record.Value...),
		Headers: encodeOriginHeaders(dead[0].Record.Headers, o),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.logs[tp].Append(Message{Value: []byte("interleaved-normal")}); err != nil {
		t.Fatal(err)
	}

	got, _ := b.replays.get("g", req.RequestID)
	b.mu.Lock()
	resumed, err := b.runReplayLocked(got, b.clock.Now())
	b.mu.Unlock()
	if err != nil {
		t.Fatalf("恢复: %v", err)
	}
	if resumed.Items[0].NewOffset != 2 || !resumed.Items[0].Appended {
		t.Fatalf("已落盘条目对账异常: %+v", resumed.Items[0])
	}
	if resumed.Items[1].NewOffset != 4 || !resumed.Items[1].Appended {
		t.Fatalf("缺失条目应追加到 off=4，实际: %+v", resumed.Items[1])
	}
	if b.logs[tp].len() != 5 {
		t.Fatalf("日志长度=%d want 5", b.logs[tp].len())
	}
}

// TestReplayPendingSurvivesRestart 验证 pending 请求在重启后仍可被查询并续跑完成。
func TestReplayPendingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	clk := &testClock{t: time.Unix(71000, 0)}
	cfg := Config{
		DataDir: dir, LeaseTTL: time.Hour, PartCapacity: 100, MaxInflight: 4,
		Retry: DefaultRetryPolicy(), Clock: clk,
	}
	b1, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tp := TopicPartition{Topic: "t", Partition: 0}
	seedDead(t, b1, tp, 2, "boom")
	req := replayRequest{
		RequestID: "rp-restart-pending", Group: "g", CreatedAt: clk.Now(),
		Status: ReplayPending,
	}
	for i := int64(0); i < 2; i++ {
		req.Items = append(req.Items, replayItemState{
			Topic: "t", Partition: 0, OldOffset: i, Base: 2, NewOffset: -1,
		})
	}
	if err := b1.replays.create(req); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：日志被自动扫描打开，pending 请求经 ResumePendingReplays 续跑。
	b2, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.ResumePendingReplays(); err != nil {
		t.Fatal(err)
	}
	r2, ok := b2.replays.get("g", "rp-restart-pending")
	if !ok {
		t.Fatal("重启后 pending 请求丢失")
	}
	got := toReplayResult(r2)
	if got.Status != ReplayCompleted {
		t.Fatalf("重启后续跑状态=%s want completed", got.Status)
	}
	if b2.logs[tp].len() != 4 {
		t.Fatalf("重启后续跑日志长度=%d want 4", b2.logs[tp].len())
	}
}
