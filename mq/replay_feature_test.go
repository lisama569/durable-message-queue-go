package mq_test

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// failOneToDLQ 让指定位移的消息完成三轮失败进入死信；其余被窗口带出的消息
// 先 Ack 挪开（它们不是本用例关注对象），不改变目标消息的重试节奏。
func failOneToDLQ(t *testing.T, b *mq.Broker, c *mq.Consumer,
	clk *fakeClock, waits []time.Duration, offset int64) {
	t.Helper()
	for _, w := range waits {
		clk.advance(w)
		_ = b.Heartbeat(c) // 失败流程可能跨越租约 TTL，测试中显式续租
		var target *mq.Delivery
		for tries := 0; tries < 20 && target == nil; tries++ {
			ds, _ := b.Poll(c, 64)
			for i := range ds {
				if ds[i].Record.Offset == offset {
					d := ds[i]
					target = &d
				} else {
					if err := b.Ack(c, ds[i]); err != nil {
						t.Fatalf("挪开无关消息失败: %v", err)
					}
				}
			}
		}
		if target == nil {
			t.Fatalf("找不到待失败的目标消息 off=%d", offset)
		}
		if err := b.Nack(c, *target, "boom"); err != nil &&
			!errors.Is(err, mq.ErrRetryExhausted) {
			t.Fatal(err)
		}
	}
}

// TestReplayByFilters 覆盖按分区、位移、时间范围、失败原因挑选重投。
func TestReplayByFilters(t *testing.T) {
	// setup 构造 4 条死信：t/p0#{0,1}(原因 boom-db-timeout)、t/p1#0(同原因)、u/p0#0(boom-other)。
	setup := func(t *testing.T) (*mq.Broker, *mq.Consumer, *fakeClock) {
		clk := &fakeClock{t: time.Unix(10000, 0)}
		cfg := withLogger(retryCfg(t, clk))
		b, _ := mq.NewBroker(cfg)
		t.Cleanup(func() { b.Close() })
		b.CreateTopic("t", 2)
		b.CreateTopic("u", 1)
		b.Produce(mq.TopicPartition{Topic: "t", Partition: 0}, mq.Message{Value: []byte("a0")})
		b.Produce(mq.TopicPartition{Topic: "t", Partition: 0}, mq.Message{Value: []byte("a1")})
		b.Produce(mq.TopicPartition{Topic: "t", Partition: 1}, mq.Message{Value: []byte("b0")})
		b.Produce(mq.TopicPartition{Topic: "u", Partition: 0}, mq.Message{Value: []byte("c0")})
		c, _ := b.JoinGroup("g", "c")
		drainAllToDLQ(t, b, c, clk)
		if n := len(b.DeadLetters("g")); n != 4 {
			t.Fatalf("死信=%d want 4", n)
		}
		return b, c, clk
	}

	// 1) 主题+分区。
	t.Run("topic_partition", func(t *testing.T) {
		b, _, _ := setup(t)
		res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{Topic: "t", Partitions: []int{1}})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Items) != 1 || res.Items[0].Partition != 1 {
			t.Fatalf("分区筛选异常: %+v", res.Items)
		}
	})

	// 2) 主题+位移。
	t.Run("offset", func(t *testing.T) {
		b, _, _ := setup(t)
		res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{Topic: "t", Offsets: []int64{1}})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Items) != 1 || res.Items[0].Partition != 0 || res.Items[0].OldOffset != 1 {
			t.Fatalf("位移筛选异常: %+v", res.Items)
		}
	})

	// 3) 失败原因子串。
	t.Run("reason", func(t *testing.T) {
		b, _, _ := setup(t)
		res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{FailureReason: "db-timeout"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Items) != 3 {
			t.Fatalf("原因筛选=%d want 3: %+v", len(res.Items), res.Items)
		}
		for _, it := range res.Items {
			if it.Topic == "u" {
				t.Fatalf("原因子串误伤 u 主题: %+v", it)
			}
		}
		// 无匹配子串。
		if _, err := b.ReplayDeadLetters("g", mq.ReplayFilter{FailureReason: "no-such-reason"}); !errors.Is(err, mq.ErrNothingToReplay) {
			t.Fatalf("无匹配原因 err=%v", err)
		}
	})

	// 4) 时间范围 + 已重投不重复。
	t.Run("time_range_and_replayed", func(t *testing.T) {
		b, _, _ := setup(t)
		dlq := b.DeadLetters("g")
		first, last := dlq[0].DeadAt, dlq[len(dlq)-1].DeadAt
		res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{
			DeadAtFrom: first, DeadAtTo: last})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Items) != 4 {
			t.Fatalf("全量时间范围=%d want 4", len(res.Items))
		}
		// 已重投：任何筛选都无候选。
		if _, err := b.ReplayDeadLetters("g", mq.ReplayFilter{
			DeadAtFrom: first.Add(-time.Hour), DeadAtTo: last.Add(time.Hour)}); !errors.Is(err, mq.ErrNothingToReplay) {
			t.Fatalf("已重投后重复重投 err=%v want ErrNothingToReplay", err)
		}
	})

	// 5) 状态查询。
	t.Run("status", func(t *testing.T) {
		b, _, _ := setup(t)
		res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{Topic: "u"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := b.ReplayStatus("g", res.RequestID)
		if err != nil || got.Status != mq.ReplayCompleted || len(got.Items) != 1 {
			t.Fatalf("ReplayStatus: %+v err=%v", got, err)
		}
		if n := len(b.ListReplays("g")); n != 1 {
			t.Fatalf("ListReplays=%d want 1", n)
		}
		if _, err := b.ReplayStatus("g", "rp-missing"); !errors.Is(err, mq.ErrReplayNotFound) {
			t.Fatalf("查询不存在请求 err=%v want ErrReplayNotFound", err)
		}
	})

	// 6) 非法参数。
	t.Run("invalid_args", func(t *testing.T) {
		b, _, _ := setup(t)
		if _, err := b.ReplayDeadLetters("", mq.ReplayFilter{}); !errors.Is(err, mq.ErrInvalidArgument) {
			t.Fatalf("空组名 err=%v", err)
		}
		now := time.Unix(10000, 0)
		if _, err := b.ReplayDeadLetters("g", mq.ReplayFilter{
			DeadAtFrom: now, DeadAtTo: now.Add(-time.Second)}); !errors.Is(err, mq.ErrInvalidArgument) {
			t.Fatalf("颠倒时间范围 err=%v", err)
		}
	})
	t.Log("判定: 分区/位移/原因/时间范围筛选、无匹配、已重投不重复、状态查询均符合预期")
}

// drainAllToDLQ 反复推进时钟并把所有在途消息 Nack，直到全部进入死信。
func drainAllToDLQ(t *testing.T, b *mq.Broker, c *mq.Consumer, clk *fakeClock) {
	t.Helper()
	for round := 0; round < 3; round++ {
		if round > 0 {
			if round == 1 {
				clk.advance(6 * time.Millisecond)
			} else {
				clk.advance(11 * time.Millisecond)
			}
		}
		for {
			ds, _ := b.Poll(c, 64)
			if len(ds) == 0 {
				break
			}
			for _, d := range ds {
				reason := "boom-db-timeout"
				if d.Record.Topic == "u" {
					reason = "boom-other"
				}
				if err := b.Nack(c, d, reason); err != nil &&
					!errors.Is(err, mq.ErrRetryExhausted) {
					t.Fatalf("nack: %v", err)
				}
			}
		}
	}
}

func drainOrder(t *testing.T, b *mq.Broker, c *mq.Consumer, rounds int) []int64 {
	t.Helper()
	var order []int64
	for i := 0; i < rounds; i++ {
		ds, _ := b.Poll(c, 4)
		if len(ds) == 0 {
			break
		}
		for _, d := range ds {
			order = append(order, d.Record.Offset)
			if err := b.Ack(c, d); err != nil {
				t.Fatalf("ack %s: %v", d.Record.MessageID(), err)
			}
		}
	}
	return order
}

// TestReplayOrderingAndBackpressure 验证：重投不回退位移、不重投已确认消息，
// 重投记录按追加顺序排在同分区现有消息之后。
func TestReplayOrderingAndBackpressure(t *testing.T) {
	clk := &fakeClock{t: time.Unix(11000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.MaxInflight = 2
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	b.CreateTopic("t", 1)
	b.Produce(tp, mq.Message{Value: []byte("poison")}) // off0 -> DLQ
	c, _ := b.JoinGroup("g", "c")
	failOneToDLQ(t, b, c, clk, []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}, 0)

	// off0 死信后再追加普通消息 off1（正常消费提交），再追加 off2 后重投，
	// 重投记录必须排在 off2 之后得到 off3，且已确认的 off1 不会被再投。
	b.Produce(tp, mq.Message{Value: []byte("normal1")})
	ds, _ := b.Poll(c, 4)
	if len(ds) != 1 || ds[0].Record.Offset != 1 {
		t.Fatalf("期望 off1 投递: %+v", ds)
	}
	if err := b.Ack(c, ds[0]); err != nil {
		t.Fatal(err)
	}
	b.Produce(tp, mq.Message{Value: []byte("normal2")})

	res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Items[0].NewOffset != 3 {
		t.Fatalf("重投新位移=%d want 3（追加到 off2 之后）", res.Items[0].NewOffset)
	}

	got := drainOrder(t, b, c, 10)
	if fmt.Sprint(got) != fmt.Sprint([]int64{2, 3}) {
		t.Fatalf("消费顺序=%v want [2 3]", got)
	}
	t.Log("判定: 同分区按日志顺序消费 off2(normal2) -> off3(重投)，已提交的 off0/off1 未重复")

	rec, ok := b.ReadRecord(tp, 3)
	if !ok {
		t.Fatal("读取重投记录失败")
	}
	if rec.Headers["x-mq-origin-offset"] != "0" || rec.Headers["x-mq-replay-hop"] != "1" {
		t.Fatalf("重投记录保留头异常: %+v", rec.Headers)
	}
}

// TestReplayFailsAgainGoesDLQWithOrigin 验证重投消息重新获得完整重试预算，
// 再次超限时仍进死信，且新死信带 Origin 链，原因链跨周期完整。
func TestReplayFailsAgainGoesDLQWithOrigin(t *testing.T) {
	clk := &fakeClock{t: time.Unix(12000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	b.CreateTopic("t", 1)
	b.Produce(tp, mq.Message{Value: []byte("poison")})
	c, _ := b.JoinGroup("g", "c")
	failOneToDLQ(t, b, c, clk, []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}, 0)

	if _, err := b.ReplayDeadLetters("g", mq.ReplayFilter{}); err != nil {
		t.Fatal(err)
	}
	for round, w := range []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond} {
		clk.advance(w)
		ds, _ := b.Poll(c, 4)
		if len(ds) != 1 || ds[0].Record.Offset != 1 {
			t.Fatalf("round %d 投递异常: %+v", round, ds)
		}
		if ds[0].Attempt != round+1 {
			t.Fatalf("round %d attempt=%d want %d（重试预算应重置）", round, ds[0].Attempt, round+1)
		}
		err := b.Nack(c, ds[0], "replay-fail-"+strconv.Itoa(round+1))
		if round == 2 {
			if !errors.Is(err, mq.ErrRetryExhausted) {
				t.Fatalf("重投消息第3次失败应再次进死信: %v", err)
			}
		} else if err != nil {
			t.Fatalf("nack: %v", err)
		}
	}
	dlq := b.DeadLetters("g")
	if len(dlq) != 2 {
		t.Fatalf("死信=%d want 2（原始1条保留 + 重投再死1条）", len(dlq))
	}
	second := dlq[1]
	if second.Offset != 1 || second.Origin == nil {
		t.Fatalf("第二条死信异常: %+v", second)
	}
	o := second.Origin
	if o.Offset != 0 || o.Hop != 1 || o.Attempts != 3 {
		t.Fatalf("Origin 链异常: %+v", o)
	}
	if len(second.Reasons) != 6 {
		t.Fatalf("跨周期原因链=%d want 6: %v", len(second.Reasons), second.Reasons)
	}
	if second.Reasons[0] != "boom" || second.Reasons[5] != "replay-fail-3" {
		t.Fatalf("原因链内容异常: %v", second.Reasons)
	}
	t.Logf("判定: 重投消息再次超限行成新死信 off=1，Origin 指回原始 off=0，完整原因链=%v",
		second.Reasons)

	res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{Offsets: []int64{1}})
	if err != nil {
		t.Fatal(err)
	}
	ds, _ := b.Poll(c, 4)
	if len(ds) != 1 {
		t.Fatalf("二次重投投递异常: %+v", ds)
	}
	oo := ds[0].ReplayInfo()
	if oo == nil || oo.Hop != 2 || oo.Offset != 0 || res.Items[0].NewOffset != 2 {
		t.Fatalf("二次重投 Origin 链异常: %+v", oo)
	}
	t.Logf("判定: 二次重投 hop=%d，原始位移=%d，失败次数与原始生产时间沿链保留", oo.Hop, oo.Offset)
}

// TestReplayCapacityBackpressure 验证分区容量不足时整批拒绝（ErrQueueFull），
// 不部分入队、不丢消息；小批量在恰好装得下时成功。
func TestReplayCapacityBackpressure(t *testing.T) {
	clk := &fakeClock{t: time.Unix(13000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.PartCapacity = 5
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	b.CreateTopic("t", 1)
	c, _ := b.JoinGroup("g", "c")
	// 逐条生产、逐条打进死信：避免首批窗口把多条同时带出。
	for i := 0; i < 4; i++ {
		b.Produce(tp, mq.Message{Value: []byte("x")})
		failOneToDLQ(t, b, c, clk, []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}, int64(i))
	}
	if n := len(b.DeadLetters("g")); n != 4 {
		t.Fatalf("死信=%d want 4", n)
	}

	_, err := b.ReplayDeadLetters("g", mq.ReplayFilter{})
	if !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("容量不足 err=%v want ErrQueueFull", err)
	}
	if st := b.LogStats()[0]; st.Count != 4 {
		t.Fatalf("被部分入队: count=%d want 4（拒绝时不应写入任何记录）", st.Count)
	}
	if n := len(b.ListReplays("g")); n != 0 {
		t.Fatalf("整批被容量拒绝时不应留下重投请求: %d", n)
	}
	t.Log("判定: 重投容量预检失败 -> ErrQueueFull，零写入、零请求残留，不静默丢弃")

	res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{Offsets: []int64{0}})
	if err != nil {
		t.Fatalf("单条重投应成功: %v", err)
	}
	if res.Items[0].NewOffset != 4 {
		t.Fatalf("新位移=%d want 4", res.Items[0].NewOffset)
	}
	_, err = b.ReplayDeadLetters("g", mq.ReplayFilter{})
	if !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("满载后重投 err=%v want ErrQueueFull", err)
	}
	t.Log("判定: 容量恰好容纳的批次成功，之后剩余 3 条继续被反压（明确而非静默）")
}

// TestReplayRestartRecovery 验证崩溃恢复：重启后已完成的重投状态恢复，
// 已落盘条目不重复追加，重投消息可被新消费者继续消费。
func TestReplayRestartRecovery(t *testing.T) {
	clk := &fakeClock{t: time.Unix(14000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.PartCapacity = 100
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	b, _ := mq.NewBroker(cfg)
	b.CreateTopic("t", 1)
	c, _ := b.JoinGroup("g", "c")
	for i := 0; i < 3; i++ {
		b.Produce(tp, mq.Message{Value: []byte("p")})
		failOneToDLQ(t, b, c, clk, []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}, int64(i))
	}
	res, err := b.ReplayDeadLetters("g", mq.ReplayFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 3 {
		t.Fatalf("重投条目=%d want 3", len(res.Items))
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	got, err := b2.ReplayStatus("g", res.RequestID)
	if err != nil {
		t.Fatalf("重启后重投状态丢失: %v", err)
	}
	if got.Status != mq.ReplayCompleted || len(got.Items) != 3 {
		t.Fatalf("重启后状态异常: %+v", got)
	}
	var total int64
	for _, s := range b2.LogStats() {
		total += s.Count
	}
	if total != 6 {
		t.Fatalf("重启后记录数=%d want 6（重启续跑不得重复追加）", total)
	}
	_, err = b2.ReplayDeadLetters("g", mq.ReplayFilter{})
	if !errors.Is(err, mq.ErrNothingToReplay) {
		t.Fatalf("重启后重复重投 err=%v", err)
	}
	c2, _ := b2.JoinGroup("g", "c-new")
	d2, _ := b2.Poll(c2, 10)
	if len(d2) != 3 {
		t.Fatalf("重启后重投消息投递数=%d want 3", len(d2))
	}
	for i, d := range d2 {
		if d.Record.Offset != int64(i+3) {
			t.Fatalf("重投位移异常: %s", d.Record.MessageID())
		}
		if d.ReplayInfo() == nil || d.ReplayInfo().Offset != int64(i) {
			t.Fatalf("重投来源丢失: %+v", d.ReplayInfo())
		}
	}
	t.Log("判定: 重启后重投完成态、逐条新位移与来源头均恢复；无重复追加、无漏投")
}

// TestReplayConcurrentIdempotent 验证多个调用方并发触发重投不会重复入队。
func TestReplayConcurrentIdempotent(t *testing.T) {
	clk := &fakeClock{t: time.Unix(15000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	cfg.PartCapacity = 100
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	b.CreateTopic("t", 1)
	const N = 5
	c, _ := b.JoinGroup("g", "c")
	for i := 0; i < N; i++ {
		b.Produce(tp, mq.Message{Value: []byte("p")})
		failOneToDLQ(t, b, c, clk, []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}, int64(i))
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := b.ReplayDeadLetters("g", mq.ReplayFilter{})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	success, nothing := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, mq.ErrNothingToReplay):
			nothing++
		default:
			t.Fatalf("并发重投意外错误: %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("成功请求数=%d want 1（其余应得到 ErrNothingToReplay）", success)
	}
	if st := b.LogStats()[0]; st.Count != int64(2*N) {
		t.Fatalf("分区记录数=%d want %d（并发重投不得重复入队）", st.Count, 2*N)
	}
	seen := map[int64]int{}
	for {
		d2, _ := b.Poll(c, 16)
		if len(d2) == 0 {
			break
		}
		for _, d := range d2 {
			seen[d.Record.Offset]++
			if err := b.Ack(c, d); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(seen) != N {
		t.Fatalf("重投消息可消费数=%d want %d", len(seen), N)
	}
	for off, n := range seen {
		if n != 1 {
			t.Fatalf("位移 %d 被投递 %d 次", off, n)
		}
	}
	t.Logf("判定: 8 个并发重投仅 1 个成功、%d 个得到 ErrNothingToReplay；死信不重不漏", nothing)
}
