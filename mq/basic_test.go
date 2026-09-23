package mq_test

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// fakeClock 是测试用可控时钟。
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func testConfig(t *testing.T) mq.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := mq.DefaultConfig(filepath.Join(dir, "data"))
	cfg.LeaseTTL = 50 * time.Millisecond
	cfg.PartCapacity = 10
	cfg.MaxInflight = 4
	cfg.Retry = mq.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
		Multiplier:  2,
	}
	return cfg
}

func withLogger(cfg mq.Config) mq.Config {
	cfg.Logger = log.New(os.Stderr, "[mq-test] ", log.LstdFlags|log.Lmicroseconds)
	return cfg
}

// TestProduceConsumeOrderedAck 验证：分区内顺序稳定、Ack 后位移幂等推进。
func TestProduceConsumeOrderedAck(t *testing.T) {
	cfg := withLogger(testConfig(t))
	b, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	defer b.Close()

	tp := mq.TopicPartition{Topic: "orders", Partition: 0}
	if err := b.CreateTopic("orders", 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		off, err := b.Produce(tp, mq.Message{Value: []byte("msg-" + strconv.Itoa(i))})
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		if off != int64(i) {
			t.Fatalf("offset = %d, want %d", off, i)
		}
		t.Logf("判定: 投递 msg-%d -> 位移=%d（追加顺序单调递增）", i, off)
	}

	c, err := b.JoinGroup("g1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	var ds []mq.Delivery
	for len(ds) < 5 {
		batch, err := b.Poll(c, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		ds = append(ds, batch...)
		// 模拟取走后立即确认，窗口才能继续推进。
		for _, d := range batch {
			if err := b.Ack(c, d); err != nil {
				t.Fatalf("ack %s: %v", d.Record.MessageID(), err)
			}
		}
	}
	if len(ds) != 5 {
		t.Fatalf("poll len = %d, want 5", len(ds))
	}
	for i, d := range ds {
		if d.Record.Offset != int64(i) {
			t.Fatalf("delivery %d offset=%d, want %d; msgID=%s",
				i, d.Record.Offset, i, d.Record.MessageID())
		}
		if d.Attempt != 1 {
			t.Fatalf("msgID=%s attempt=%d want 1", d.Record.MessageID(), d.Attempt)
		}
		t.Logf("判定: 首次投递 %s attempt=%d consumer=%s（同分区顺序与生产顺序一致）",
			d.Record.MessageID(), d.Attempt, d.ConsumerID)
	}
	// 全部确认后再次 poll：无新消息，且不能重复投递。
	again, _ := b.Poll(c, 10)
	if len(again) != 0 {
		t.Fatalf("已确认消息被重复投递: %d", len(again))
	}
	for _, d := range ds {
		// 重复 Ack 必须幂等成功。
		if err := b.Ack(c, d); err != nil {
			t.Fatalf("重复 ack %s 非幂等: %v", d.Record.MessageID(), err)
		}
		t.Logf("判定: 重复确认 %s -> 幂等成功", d.Record.MessageID())
	}
}

// TestRestartRedelivery 验证：未确认消息在重启后重新投递；
// 已确认位移持久化，重启后不会重复。
func TestRestartRedelivery(t *testing.T) {
	cfg := withLogger(testConfig(t))
	tp := mq.TopicPartition{Topic: "orders", Partition: 0}

	b, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders", 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := b.Produce(tp, mq.Message{Value: []byte("m" + strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := b.JoinGroup("g1", "c1")
	ds, _ := b.Poll(c, 4)
	// 只确认前两条 -> committed offset 应推进到 2。
	if err := b.Ack(c, ds[0]); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack(c, ds[1]); err != nil {
		t.Fatal(err)
	}
	t.Logf("判定: 关闭前确认 %s,%s；%s,%s 未确认（预期重启后重投）",
		ds[0].Record.MessageID(), ds[1].Record.MessageID(),
		ds[2].Record.MessageID(), ds[3].Record.MessageID())
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：同一数据目录，新 Broker + 新消费者。
	b2, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("orders", 1); err != nil {
		t.Fatal(err)
	}
	c2, _ := b2.JoinGroup("g1", "c-new")
	redelivered, err := b2.Poll(c2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(redelivered) != 2 {
		t.Fatalf("重启后重投数=%d, want 2", len(redelivered))
	}
	for i, d := range redelivered {
		if d.Record.Offset != int64(i+2) {
			t.Fatalf("重投 offset=%d want %d msgID=%s",
				d.Record.Offset, i+2, d.Record.MessageID())
		}
		t.Logf("判定: 重启后重新投递 %s attempt=%d（位移从已提交点 2 继续，无静默跳过）",
			d.Record.MessageID(), d.Attempt)
	}
}

// TestCommitOffsetValidation 验证回退与越界提交被明确拒绝，且错误可区分。
func TestCommitOffsetValidation(t *testing.T) {
	cfg := withLogger(testConfig(t))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := b.Produce(tp, mq.Message{Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := b.JoinGroup("g", "c")
	if _, err := b.Poll(c, 3); err != nil {
		t.Fatal(err)
	}
	// 提交到 3（logEnd）成功。
	if err := b.CommitOffset(c, tp, 3); err != nil {
		t.Fatalf("commit 3: %v", err)
	}
	t.Logf("判定: 提交 offset=3 logEnd=3 -> 接受（幂等位点）")

	err := b.CommitOffset(c, tp, 2)
	if !errors.Is(err, mq.ErrOffsetRolledBack) {
		t.Fatalf("回退提交 err=%v, want ErrOffsetRolledBack", err)
	}
	t.Logf("判定: 提交 offset=2 < committed=3 -> 拒绝原因 ErrOffsetRolledBack")

	err = b.CommitOffset(c, tp, 100)
	if !errors.Is(err, mq.ErrOffsetOutOfRange) {
		t.Fatalf("越界提交 err=%v, want ErrOffsetOutOfRange", err)
	}
	t.Logf("判定: 提交 offset=100 > logEnd=3 -> 拒绝原因 ErrOffsetOutOfRange")

	// 相同位移重复提交幂等成功。
	if err := b.CommitOffset(c, tp, 3); err != nil {
		t.Fatalf("重复提交非幂等: %v", err)
	}
	t.Logf("判定: 重复提交 offset=3 -> 幂等成功")
}

// TestCapacityAndWatermark 验证容量反压：满队列 Produce 被拒绝而非静默丢弃。
func TestCapacityAndWatermark(t *testing.T) {
	cfg := withLogger(testConfig(t)) // capacity=10
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "cap", Partition: 0}
	if err := b.CreateTopic("cap", 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := b.Produce(tp, mq.Message{Value: []byte("v")}); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	_, err := b.Produce(tp, mq.Message{Value: []byte("overflow")})
	if !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("满队列 produce err=%v, want ErrQueueFull", err)
	}
	st := b.LogStats()[0]
	if !st.Full || st.Used != 1.0 || st.Count != 10 {
		t.Fatalf("stats=%+v", st)
	}
	t.Logf("判定: 分区容量=%d 已满 used=%.2f full=%v -> 溢出被明确拒绝，消息未静默丢弃",
		st.Capacity, st.Used, st.Full)
}

// TestConcurrentConsumers 基本并发消费冒烟：同组多消费者下消息不重不漏。
func TestConcurrentConsumers(t *testing.T) {
	cfg := withLogger(testConfig(t))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	if err := b.CreateTopic("work", 4); err != nil {
		t.Fatal(err)
	}
	const N = 20
	for i := 0; i < N; i++ {
		tp := mq.TopicPartition{Topic: "work", Partition: i % 4}
		if _, err := b.Produce(tp, mq.Message{Value: []byte("job" + strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[string]int{}
	consumers := make([]*mq.Consumer, 4)
	// 先让全部消费者完成注册，再统一开始拉取，
	// 避免懒分配时成员尚未到齐而把分区粘滞给先到者。
	for i := 0; i < 4; i++ {
		c, err := b.JoinGroup("workers", "w"+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		consumers[i] = c
	}
	for _, c := range consumers {
		wg.Add(1)
		go func(cons *mq.Consumer) {
			defer wg.Done()
			emptyRounds := 0
			for emptyRounds < 5 {
				ds, err := b.Poll(cons, 4)
				if err != nil {
					t.Errorf("poll: %v", err)
					return
				}
				if len(ds) == 0 {
					emptyRounds++
					time.Sleep(time.Millisecond)
					continue
				}
				emptyRounds = 0
				for _, d := range ds {
					mu.Lock()
					seen[d.Record.MessageID()]++
					mu.Unlock()
					if err := b.Ack(cons, d); err != nil {
						t.Errorf("ack %s: %v", d.Record.MessageID(), err)
					}
				}
			}
		}(c)
	}
	wg.Wait()
	if len(seen) != N {
		t.Fatalf("唯一消息数=%d want %d（有丢失或重复）", len(seen), N)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("消息 %s 被处理 %d 次", id, n)
		}
	}
	t.Logf("判定: %d 条消息被 4 个消费者恰好各处理一次（不丢不重）", N)
}
