package mq_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// TestPartitionExclusivity 验证同组分区独占：任一时刻一个分区只有一个持有者，
// 非持有者既拉不到该分区消息，也无法确认他人的在途投递。
func TestPartitionExclusivity(t *testing.T) {
	cfg := withLogger(testConfig(t))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	if err := b.CreateTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	tp0 := mq.TopicPartition{Topic: "t", Partition: 0}
	tp1 := mq.TopicPartition{Topic: "t", Partition: 1}
	for i := 0; i < 4; i++ {
		b.Produce(tp0, mq.Message{Value: []byte("a")})
		b.Produce(tp1, mq.Message{Value: []byte("b")})
	}
	// 先到齐再拉取，分配确定：sorted(c1,c2)，tp0->c1, tp1->c2。
	c1, _ := b.JoinGroup("g", "c1")
	c2, _ := b.JoinGroup("g", "c2")

	d1, _ := b.Poll(c1, 8)
	d2, _ := b.Poll(c2, 8)
	for _, d := range d1 {
		if d.Record.Partition != 0 {
			t.Fatalf("c1 拿到非自己分区: %s", d.Record.MessageID())
		}
	}
	for _, d := range d2 {
		if d.Record.Partition != 1 {
			t.Fatalf("c2 拿到非自己分区: %s", d.Record.MessageID())
		}
	}
	as := b.Assignments("g")
	if as[tp0] != "c1" || as[tp1] != "c2" {
		t.Fatalf("独占关系异常: %v", as)
	}
	if len(as) != 2 {
		t.Fatalf("分区数=%d want 2", len(as))
	}
	t.Logf("判定: 分区独占关系 tp0=%s tp1=%s，无任一分区被两个消费者同时持有", as[tp0], as[tp1])

	// c2 试图确认 c1 的投递：必须被明确拒绝（非持有者）。
	if len(d1) > 0 {
		err := b.Ack(c2, d1[0])
		if !errors.Is(err, mq.ErrNotOwner) {
			t.Fatalf("非持有者 ack err=%v, want ErrNotOwner", err)
		}
		t.Logf("判定: c2 确认 %s 被拒绝，原因 ErrNotOwner", d1[0].Record.MessageID())
	}
}

// TestLeaseExpiryRebalance 验证租约过期后安全再分配：
// 旧主未确认的消息重投给新主，旧主的延迟确认被代次栅栏拒绝，杜绝双持与跳过。
func TestLeaseExpiryRebalance(t *testing.T) {
	clk := &fakeClock{t: time.Unix(3000, 0)}
	cfg := testConfig(t)
	cfg.Clock = clk
	cfg = withLogger(cfg)
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	for i := 0; i < 3; i++ {
		b.Produce(tp, mq.Message{Value: []byte("m")})
	}
	c1, _ := b.JoinGroup("g", "c1")
	d1, _ := b.Poll(c1, 3)
	if len(d1) != 3 {
		t.Fatalf("c1 首批=%d want 3", len(d1))
	}
	t.Logf("判定: c1 持有分区并取走 %d 条未确认消息；随后崩溃（停心跳）", len(d1))

	// 超过租约 TTL，c2 加入并拉取 -> 触发过期回收与再平衡。
	clk.advance(2 * cfg.LeaseTTL)
	c2, _ := b.JoinGroup("g", "c2")
	redelivered, err := b.Poll(c2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(redelivered) != 3 {
		t.Fatalf("再平衡后重投数=%d want 3（旧主未确认消息不得丢失/跳过）", len(redelivered))
	}
	for i, d := range redelivered {
		if d.Record.Offset != int64(i) || d.ConsumerID != "c2" {
			t.Fatalf("重投异常: %+v", d)
		}
		t.Logf("判定: %s 再分配给 c2 重新投递（位移连续、无静默跳过）", d.Record.MessageID())
	}
	as := b.Assignments("g")
	if as[tp] != "c2" {
		t.Fatalf("再平衡后 owner=%s want c2", as[tp])
	}

	// 旧主 c1“诈尸”般迟到的确认：必须被明确拒绝（成员已失效/旧会话），
	// 不能与 c2 双持。
	err = b.Ack(c1, d1[0])
	if !errors.Is(err, mq.ErrStaleGeneration) &&
		!errors.Is(err, mq.ErrLeaseExpired) &&
		!errors.Is(err, mq.ErrUnknownConsumer) {
		t.Fatalf("旧主迟到确认 err=%v, 应为失效会话类错误", err)
	}
	t.Logf("判定: 旧主 c1 的迟到确认被拒绝，原因=%v（失效会话栅栏防止双持与重复提交）", err)

	// 新主确认成功，提交位推进。
	for _, d := range redelivered {
		if err := b.Ack(c2, d); err != nil {
			t.Fatal(err)
		}
	}
}

// TestHeartbeatKeepsOwnership 验证租约内正常心跳不会把分区让给他人。
func TestHeartbeatKeepsOwnership(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4000, 0)}
	cfg := withLogger(testConfig(t))
	cfg.Clock = clk
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	b.Produce(tp, mq.Message{Value: []byte("x")})

	c1, _ := b.JoinGroup("g", "c1")
	c2, _ := b.JoinGroup("g", "c2")
	if _, err := b.Poll(c1, 1); err != nil {
		t.Fatal(err)
	}
	// 时间推进但每次都续租，c1 始终是 owner。
	for i := 0; i < 5; i++ {
		clk.advance(cfg.LeaseTTL / 2)
		if err := b.Heartbeat(c1); err != nil {
			t.Fatal(err)
		}
		b.Step()
	}
	if got := b.Assignments("g")[tp]; got != "c1" {
		t.Fatalf("持续心跳后 owner=%s want c1", got)
	}
	if d2, _ := b.Poll(c2, 1); len(d2) != 0 {
		t.Fatalf("租约有效时 c2 不应抢到 c1 的分区: %+v", d2)
	}
	t.Log("判定: c1 持续心跳续租，分区不被 c2 抢占")
}

// TestLeaveGroupReassign 验证主动离开立即释放分区，新主可接管。
func TestLeaveGroupReassign(t *testing.T) {
	cfg := withLogger(testConfig(t))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	b.Produce(tp, mq.Message{Value: []byte("x")})
	c1, _ := b.JoinGroup("g", "c1")
	d1, _ := b.Poll(c1, 1)
	if len(d1) != 1 {
		t.Fatal("c1 应取到消息")
	}
	c2, _ := b.JoinGroup("g", "c2")
	b.LeaveGroup(c1)
	t.Log("判定: c1 主动离开，其在途消息被回收，分区立即进入可再分配状态")

	d2, _ := b.Poll(c2, 1)
	if len(d2) != 1 || d2[0].Record.Offset != 0 {
		t.Fatalf("c2 接管重投异常: %+v", d2)
	}
	t.Logf("判定: %s 由 c2 接管重新投递（旧主在途被回收，无丢失无跳过）",
		d2[0].Record.MessageID())

	// 旧主离开后其迟到确认必须失败。
	if err := b.Ack(c1, d1[0]); err == nil {
		t.Fatal("已离开的 c1 确认竟然成功")
	} else {
		t.Logf("判定: 已离开 c1 的迟到确认被拒绝，原因=%v", err)
	}
	if err := b.Ack(c2, d2[0]); err != nil {
		t.Fatalf("新主 c2 确认失败: %v", err)
	}
}

// TestRestartDLQAndOffsets 验证重启后死信记录与位移状态均从磁盘恢复。
func TestRestartDLQAndOffsets(t *testing.T) {
	clk := &fakeClock{t: time.Unix(5000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	b.CreateTopic("t", 1)
	b.Produce(tp, mq.Message{Value: []byte("poison")})
	c, _ := b.JoinGroup("g", "c")
	waits := []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}
	for round, w := range waits {
		if w > 0 {
			clk.advance(w)
		}
		ds, _ := b.Poll(c, 1)
		if len(ds) != 1 {
			t.Fatalf("round %d 投递异常", round)
		}
		_ = b.Nack(c, ds[0], "boom")
	}
	if n := len(b.DeadLetters("g")); n != 1 {
		t.Fatalf("死信=%d want 1", n)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：死信记录、消息日志都应从磁盘恢复。
	b2, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	dlq := b2.DeadLetters("g")
	if len(dlq) != 1 || string(dlq[0].Record.Value) != "poison" || len(dlq[0].Reasons) != 3 {
		t.Fatalf("重启后死信恢复异常: %+v", dlq)
	}
	rec, ok := b2.ReadRecord(tp, 0)
	if !ok || string(rec.Value) != "poison" {
		t.Fatal("重启后消息日志未恢复")
	}
	t.Logf("判定: 重启后死信仍在（%d 条原因链=%v），分区日志位移0记录完整可读",
		len(dlq[0].Reasons), dlq[0].Reasons)
}

// TestDuplicateConsumerRejected 验证同组消费者 ID 不可重复注册。
func TestDuplicateConsumerRejected(t *testing.T) {
	cfg := withLogger(testConfig(t))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	if _, err := b.JoinGroup("g", "same"); err != nil {
		t.Fatal(err)
	}
	_, err := b.JoinGroup("g", "same")
	if !errors.Is(err, mq.ErrConsumerRegistered) {
		t.Fatalf("重复注册 err=%v, want ErrConsumerRegistered", err)
	}
	t.Log("判定: 同组重复 consumerID 注册被拒绝，原因 ErrConsumerRegistered")
}
