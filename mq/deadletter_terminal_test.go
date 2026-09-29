package mq_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// killDeliveryToDLQ 对一条【已经在途】的投递，按确定性退避把它连续失败到
// 进死信；期间每轮 Poll 都只应取到这一条重试投递（其余在途消息保持不动）。
func killDeliveryToDLQ(t *testing.T, b *mq.Broker, c *mq.Consumer, clk *fakeClock,
	first mq.Delivery, reason string) {
	t.Helper()
	d := first
	if err := b.Nack(c, d, reason); err != nil {
		t.Fatalf("nack attempt1: %v", err)
	}
	for attempt := 2; attempt <= 3; attempt++ {
		switch attempt {
		case 2:
			clk.advance(6 * time.Millisecond)
		case 3:
			clk.advance(11 * time.Millisecond)
		}
		ds, err := b.Poll(c, 8)
		if err != nil {
			t.Fatalf("attempt=%d poll: %v", attempt, err)
		}
		if len(ds) != 1 || ds[0].Record.Offset != first.Record.Offset {
			t.Fatalf("attempt=%d 应只重投位移 %d，实际 %+v",
				attempt, first.Record.Offset, ds)
		}
		if ds[0].Attempt != attempt {
			t.Fatalf("attempt=%d 投递计数=%d", attempt, ds[0].Attempt)
		}
		d = ds[0]
		err = b.Nack(c, d, reason)
		if attempt == 3 {
			if !errors.Is(err, mq.ErrRetryExhausted) {
				t.Fatalf("第3次失败应进死信: %v", err)
			}
		} else if err != nil {
			t.Fatalf("attempt=%d nack: %v", attempt, err)
		}
	}
}

func deliveryOffsets(ds []mq.Delivery) []int64 {
	out := make([]int64, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.Record.Offset)
	}
	return out
}

func countOffsets(ds []mq.Delivery, off int64) int {
	n := 0
	for _, d := range ds {
		if d.Record.Offset == off {
			n++
		}
	}
	return n
}

// terminalDLQConfig 构造租约足够长、退避 5ms/10ms、3 次上限的可控时钟配置。
func terminalDLQConfig(t *testing.T, clk *fakeClock) mq.Config {
	cfg := retryCfg(t, clk)
	cfg.LeaseTTL = time.Hour
	return withLogger(cfg)
}

// TestDLQTerminalAcrossRestart 覆盖故障演练场景一：
// 同分区里死信【前面】还有没确认的消息（0、2 在途），中间的 1 被重试到进死信，
// 连续前缀提交被未确认的 0 卡在 0；随后消费者崩溃重启。
// 断言：重启后已进死信的原位移 1 不会被再次投递（更不会 attempt 从 1 重新计数），
// 只有此前未确认的 0/2 被重投；死信不重复产生；确认 0/2 后提交位能一路越过
// 死信推进到日志末尾，不被死信卡住，也不会跳过任何从未投递的消息。
func TestDLQTerminalAcrossRestart(t *testing.T) {
	clk := &fakeClock{t: time.Unix(30000, 0)}
	cfg := terminalDLQConfig(t, clk)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	c, _ := b.JoinGroup("g", "c")
	for i := 0; i < 3; i++ {
		if _, err := b.Produce(tp, mq.Message{Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	// 首批一次取走 0/1/2（MaxInflight=4）。
	first, _ := b.Poll(c, 8)
	if len(first) != 3 {
		t.Fatalf("首批=%d want 3", len(first))
	}
	// 只把中间的 1 连续失败到死信；0、2 始终保持在途未确认。
	var d1 mq.Delivery
	for _, d := range first {
		if d.Record.Offset == 1 {
			d1 = d
		}
	}
	killDeliveryToDLQ(t, b, c, clk, d1, "boom")
	if n := len(b.DeadLetters("g")); n != 1 {
		t.Fatalf("死信数=%d want 1", n)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 崩溃后用同一数据目录重启，新消费者重新加入。
	b2, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	c2, _ := b2.JoinGroup("g", "c-new")
	got, err := b2.Poll(c2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("重启后应只重投此前未确认的 0/2，实际 %+v", deliveryOffsets(got))
	}
	if offs := deliveryOffsets(got); offs[0] != 0 || offs[1] != 2 {
		t.Fatalf("重投位移异常=%v want [0 2]（死信位移1不得出现）", offs)
	}
	for _, d := range got {
		if d.Attempt != 1 {
			t.Fatalf("未确认消息重投 attempt=%d，消息 %s 应为1", d.Attempt, d.Record.MessageID())
		}
	}
	t.Logf("判定: 重启后仅重投在途的 %v，已死信位移1未被再投", deliveryOffsets(got))

	// 再多轮推进时间并轮询：死信位移 1 永远不能再被投出，也不能产生第二条死信。
	for i := 0; i < 5; i++ {
		clk.advance(20 * time.Millisecond)
		if err := b2.Heartbeat(c2); err != nil {
			t.Fatal(err)
		}
		more, _ := b2.Poll(c2, 8)
		if n := countOffsets(more, 1); n != 0 {
			t.Fatalf("死信位移1被再次投递 %d 次: %+v", n, more)
		}
	}
	if n := len(b2.DeadLetters("g")); n != 1 {
		t.Fatalf("同一份消息重复产生死信: %d want 1", n)
	}

	// 确认两条重投：提交位必须能越过已终态的死信位移 1 推进到 3，不被卡住。
	for _, d := range got {
		if err := b2.Ack(c2, d); err != nil {
			t.Fatalf("ack %s: %v", d.Record.MessageID(), err)
		}
	}
	if err := b2.CommitOffset(c2, tp, 3); err != nil {
		t.Fatalf("提交位应能越过死信推进到3: %v", err)
	}
	if more, _ := b2.Poll(c2, 8); len(more) != 0 {
		t.Fatalf("全部处理完不应再有投递: %+v", more)
	}
	t.Log("判定: 死信位移1在重启后保持终态；确认0/2后提交位越过死信到3，无卡住无跳过")
}

// TestDLQTerminalAcrossLeaseTakeover 覆盖故障演练场景二：
// 死信【前面】的消息尚未确认，消费者心跳超时被同组另一个消费者接管。
func TestDLQTerminalAcrossLeaseTakeover(t *testing.T) {
	clk := &fakeClock{t: time.Unix(31000, 0)}
	cfg := withLogger(retryCfg(t, clk)) // 显式使用可控的 50ms 短租约
	cfg.LeaseTTL = 50 * time.Millisecond
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	c1, _ := b.JoinGroup("g", "c1")
	for i := 0; i < 3; i++ {
		b.Produce(tp, mq.Message{Value: []byte("v")})
	}
	first, _ := b.Poll(c1, 8)
	if len(first) != 3 {
		t.Fatalf("首批=%d want 3", len(first))
	}
	// 0、2 保持在途；把 1 打进死信（整个脚本在租约内完成）。
	var d1 mq.Delivery
	for _, d := range first {
		if d.Record.Offset == 1 {
			d1 = d
		}
	}
	killDeliveryToDLQ(t, b, c1, clk, d1, "boom")
	if n := len(b.DeadLetters("g")); n != 1 {
		t.Fatalf("死信数=%d want 1", n)
	}

	// c1 崩溃停心跳，超过 TTL；c2 加入并拉取，触发过期回收与再分配。
	clk.advance(2 * cfg.LeaseTTL)
	c2, _ := b.JoinGroup("g", "c2")
	got, err := b.Poll(c2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("接管后应只重投在途的 0/2，实际 %+v", deliveryOffsets(got))
	}
	if offs := deliveryOffsets(got); offs[0] != 0 || offs[1] != 2 {
		t.Fatalf("接管后重投位移异常=%v want [0 2]", offs)
	}
	for _, d := range got {
		if d.ConsumerID != "c2" {
			t.Fatalf("接管后投递应归属 c2，实际 %s", d.ConsumerID)
		}
	}
	t.Logf("判定: c1 心跳超时后 c2 接管，重投 %v 给 c2，死信位移1未被再投", deliveryOffsets(got))

	// 旧主 c1 迟到的确认被会话栅栏拒绝（不回归）。
	if err := b.Ack(c1, first[0]); err == nil {
		t.Fatal("过期旧主的迟到确认竟然成功")
	}

	// 多轮轮询 + 时间推进：死信位移 1 不再投、死信不重复。
	for i := 0; i < 5; i++ {
		clk.advance(cfg.LeaseTTL / 2)
		_ = b.Heartbeat(c2)
		b.Step()
		more, _ := b.Poll(c2, 8)
		if n := countOffsets(more, 1); n != 0 {
			t.Fatalf("接管后死信位移1被再次投递 %d 次: %+v", n, more)
		}
	}
	if n := len(b.DeadLetters("g")); n != 1 {
		t.Fatalf("接管场景下死信被重复产生: %d want 1", n)
	}
	for _, d := range got {
		if err := b.Ack(c2, d); err != nil {
			t.Fatalf("ack %s: %v", d.Record.MessageID(), err)
		}
	}
	if err := b.CommitOffset(c2, tp, 3); err != nil {
		t.Fatalf("提交位应越过死信推进到3: %v", err)
	}
	t.Log("判定: 租约接管后死信位移保持终态，提交位可越过死信推进，独占栅栏不回归")
}

// TestRestartRedeliveryAckable 回归：重启后从日志恢复的记录必须携带
// Topic/Partition，新主对重投消息的 Ack/Nack 归属校验才能通过。
func TestRestartRedeliveryAckable(t *testing.T) {
	clk := &fakeClock{t: time.Unix(32000, 0)}
	cfg := terminalDLQConfig(t, clk)
	tp := mq.TopicPartition{Topic: "orders", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	b.CreateTopic("orders", 1)
	for i := 0; i < 2; i++ {
		b.Produce(tp, mq.Message{Value: []byte("m")})
	}
	c1, _ := b.JoinGroup("g", "c1")
	ds, _ := b.Poll(c1, 8)
	if len(ds) != 2 {
		t.Fatalf("首批=%d want 2", len(ds))
	}
	// 一条都不确认，直接重启。
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	b2.CreateTopic("orders", 1)
	c2, _ := b2.JoinGroup("g", "c2")
	got, err := b2.Poll(c2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("重投数=%d want 2", len(got))
	}
	for _, d := range got {
		if d.Record.Topic != "orders" || d.Record.Partition != 0 {
			t.Fatalf("重启恢复记录缺少主题/分区: %+v", d.Record)
		}
		// 失败一次再确认：Nack 与 Ack 都依赖 Record 的归属信息。
		if err := b2.Nack(c2, d, "once"); err != nil {
			t.Fatalf("重启后 Nack 失败（归属信息丢失）: %v", err)
		}
	}
	clk.advance(6 * time.Millisecond)
	rt, _ := b2.Poll(c2, 8)
	if len(rt) != 2 {
		t.Fatalf("退避后重投数=%d want 2: %+v", len(rt), rt)
	}
	for _, d := range rt {
		if d.Attempt != 2 {
			t.Fatalf("重投 attempt=%d want 2", d.Attempt)
		}
		if err := b2.Ack(c2, d); err != nil {
			t.Fatalf("重启后 Ack 失败（归属信息丢失）: %v", err)
		}
	}
	if err := b2.CommitOffset(c2, tp, 2); err != nil {
		t.Fatalf("提交位推进到2: %v", err)
	}
	t.Log("判定: 重启恢复的记录携带主题/分区，Nack→退避→Ack→提交全链路可用")
}
