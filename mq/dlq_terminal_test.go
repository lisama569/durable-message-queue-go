package mq_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// nackToDLQAtOffset 把首轮批次 first 中位移为 wantOff 的消息连续失败到进死信。
// 与 killToDLQ 的区别：首轮批次里可以同时压着别的未确认消息
// （例如死信前面还有没来得及确认的消息）；后续轮次退避到期，目标走重投分支。
func nackToDLQAtOffset(t *testing.T, b *mq.Broker, c *mq.Consumer, clk *fakeClock,
	first []mq.Delivery, wantOff int64, reason string) mq.Delivery {
	t.Helper()
	var target mq.Delivery
	found := false
	for _, d := range first {
		if d.Record.Offset == wantOff {
			target = d
			found = true
		}
	}
	if !found {
		t.Fatalf("首轮批次中未找到位移 %d: %+v", wantOff, first)
	}

	// 首轮失败（使用首批 Delivery）。
	if err := b.Nack(c, target, reason); err != nil {
		t.Fatalf("nack round=0: %v", err)
	}
	// 后续两轮：推进退避 -> Poll 取回到期重投 -> 再 Nack。
	for round, w := range []time.Duration{6 * time.Millisecond, 11 * time.Millisecond} {
		clk.advance(w)
		ds, err := b.Poll(c, 8)
		if err != nil {
			t.Fatalf("poll round=%d: %v", round+1, err)
		}
		var redelivered mq.Delivery
		var found bool
		for _, d := range ds {
			if d.Record.Offset == wantOff {
				redelivered = d
				found = true
			}
		}
		if !found {
			t.Fatalf("第 %d 轮未在投递批次中找到位移 %d: %+v", round+1, wantOff, ds)
		}
		err = b.Nack(c, redelivered, reason)
		if round == 1 {
			if !errors.Is(err, mq.ErrRetryExhausted) {
				t.Fatalf("第 3 次失败应进死信，err=%v", err)
			}
		} else if err != nil {
			t.Fatalf("nack round=%d: %v", round+1, err)
		}
		target = redelivered
	}
	return target
}

// TestDeadLetterTerminalAcrossRestart 复现故障演练问题一（重启场景）：
// 同分区里死信（offset1）前面还压着一条没来得及确认的消息（offset0），
// 消费者在把 offset1 重试进死信后立即崩溃。重启后：
//   - offset0 作为普通未确认消息必须被重投（至少一次，不跳过）；
//   - offset1 已是死信终态，绝不能作为新消息再投出来（attempt 从 1 重来）；
//   - offset2 正常投递、确认；全部完成后 Poll 为空；
//   - 死信记录始终只有 1 条（同一份消息不会重复产生死信）。
func TestDeadLetterTerminalAcrossRestart(t *testing.T) {
	clk := &fakeClock{t: time.Unix(21000, 0)}
	cfg := retryCfg(t, clk)
	cfg.LeaseTTL = time.Hour // 必须在 retryCfg 之后覆盖（retryCfg 会重置租约）
	cfg = withLogger(cfg)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	b.CreateTopic("t", 1)
	c, _ := b.JoinGroup("g", "c1")

	// offset0 与 offset1 同批取走（在途窗口=4）；offset0 始终不确认。
	b.Produce(tp, mq.Message{Value: []byte("unacked")})
	b.Produce(tp, mq.Message{Value: []byte("poison")})
	first, err := b.Poll(c, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].Record.Offset != 0 || first[1].Record.Offset != 1 {
		t.Fatalf("首批投递异常: %+v", first)
	}
	// offset1 三次失败进入死信；offset0 一直挂着，提交位只能停在 0。
	nackToDLQAtOffset(t, b, c, clk, first, 1, "boom")
	if off, ok := b.CommittedOffset("g", tp); !ok || off != 0 {
		t.Fatalf("offset0 未确认时提交位必须停在 0，实际 off=%d ok=%v", off, ok)
	}
	if dlq := b.DeadLetters("g"); len(dlq) != 1 || dlq[0].Offset != 1 {
		t.Fatalf("崩溃前死信异常: %+v", dlq)
	}
	t.Log("判定: offset1 已进死信，但前面的 offset0 尚未确认 -> 提交位=0；随后消费者崩溃")

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：新 Broker、新消费者；游标从提交位 0 重新开始扫描。
	b2, err := mq.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	// 重启后、消费前先再造一条新消息 offset2，验证游标扫过死信后能继续正常首发。
	if off, err := b2.Produce(tp, mq.Message{Value: []byte("fresh")}); err != nil || off != 2 {
		t.Fatalf("produce offset2: off=%d err=%v", off, err)
	}
	c2, _ := b2.JoinGroup("g", "c2")
	clk.advance(time.Second)

	got := pollAll(t, b2, c2)
	// offset0 必须重投；offset1（死信）绝不能出现；offset2 必须正常首发。
	if len(got) != 2 {
		t.Fatalf("重启后投递数=%d want 2（offset0 重投 + offset2 首发，offset1 死信跳过）: %+v",
			len(got), got)
	}
	if got[0].Record.Offset != 0 || got[1].Record.Offset != 2 {
		t.Fatalf("重启后投递位移异常: %s,%s（死信位移 1 被复活）",
			got[0].Record.MessageID(), got[1].Record.MessageID())
	}
	if got[0].Attempt != 1 {
		t.Fatalf("offset0 是普通未确认消息，重启后按新投递 attempt=1，实际 %d", got[0].Attempt)
	}
	if got[1].Attempt != 1 {
		t.Fatalf("offset2 是从未投递过的新消息，attempt 必须=1，实际 %d", got[1].Attempt)
	}
	t.Logf("判定: 重启后 %s 重投、%s 首发，死信位移 1 未被再投（attempt 未重置）",
		got[0].Record.MessageID(), got[1].Record.MessageID())

	// 即使立即再 Poll（死信已被游标越过），offset1 依然不会出现。
	if more, _ := b2.Poll(c2, 8); len(more) != 0 {
		t.Fatalf("重启后多余投递（疑似死信复活）: %+v", more)
	}

	// 确认两条消息，提交位应越过死信一路推进到日志末尾 3，不被死信卡住。
	for _, d := range got {
		if err := b2.Ack(c2, d); err != nil {
			t.Fatalf("ack %s: %v", d.Record.MessageID(), err)
		}
	}
	if err := b2.CommitOffset(c2, tp, 3); err != nil {
		t.Fatalf("提交位应可越过死信推进到 3: %v", err)
	}
	if more, _ := b2.Poll(c2, 8); len(more) != 0 {
		t.Fatalf("全部确认后仍有投递: %+v", more)
	}

	// 同一份消息没有重复入死信：死信记录仍只有 1 条，且仍指向 offset1。
	dlq := b2.DeadLetters("g")
	if len(dlq) != 1 || dlq[0].Offset != 1 {
		t.Fatalf("重启消费后死信应仍只有 1 条且指向 offset1，实际: %+v", dlq)
	}
	t.Log("判定: 死信位移 1 在重启+游标回退后保持终态，提交位越过它推进到 3，死信不重复")
}

// pollAll 反复 Poll 直到没有新投递，收集全部可投递消息（顺序即分区扫描顺序）。
func pollAll(t *testing.T, b *mq.Broker, c *mq.Consumer) []mq.Delivery {
	t.Helper()
	var all []mq.Delivery
	for i := 0; i < 50; i++ {
		ds, err := b.Poll(c, 8)
		if err != nil {
			t.Fatalf("pollAll: %v", err)
		}
		if len(ds) == 0 {
			return all
		}
		all = append(all, ds...)
	}
	t.Fatalf("pollAll 超过 50 轮仍有投递，疑似无限重投: %+v", all)
	return nil
}

// TestDeadLetterTerminalAcrossTakeover 复现故障演练问题一（接管场景）：
// 同分区里死信（offset1）前面压着一条未确认消息（offset0），随后消费者
// 心跳超时，被同组另一个消费者接管。接管后：
//   - offset0 作为旧主未确认消息重投给新主（在途回收，不跳过）；
//   - offset1 的死信终态保持，绝不当新消息再投、attempt 不从 1 重来；
//   - offset2 正常首发；同一份消息不会重复产生死信（死信仍只有 1 条）；
//   - 独占/租约栅栏不变：旧主迟到的 Ack 被拒绝，新主可正常确认并推进提交位。
func TestDeadLetterTerminalAcrossTakeover(t *testing.T) {
	clk := &fakeClock{t: time.Unix(22000, 0)}
	cfg := retryCfg(t, clk)
	cfg.LeaseTTL = 50 * time.Millisecond // 用短租约，便于可控地制造过期接管
	cfg = withLogger(cfg)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)

	c1, _ := b.JoinGroup("g", "c1")
	b.Produce(tp, mq.Message{Value: []byte("unacked")})
	b.Produce(tp, mq.Message{Value: []byte("poison")})
	first, err := b.Poll(c1, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("首批应取到 2 条，实际 %+v", first)
	}
	// offset1 三次失败进死信；offset0 一直不确认，提交位停在 0。
	nackToDLQAtOffset(t, b, c1, clk, first, 1, "boom")
	if off, _ := b.CommittedOffset("g", tp); off != 0 {
		t.Fatalf("offset0 未确认时提交位必须停在 0，实际 %d", off)
	}
	if dlq := b.DeadLetters("g"); len(dlq) != 1 || dlq[0].Offset != 1 {
		t.Fatalf("接管前死信异常: %+v", dlq)
	}
	t.Log("判定: offset1 已死信、offset0 在途未确认；c1 停心跳，等待被接管")

	// 时间越过租约 TTL，c1 心跳超时（模拟消费者崩溃）。
	clk.advance(2 * cfg.LeaseTTL)
	b.Step() // 显式推进过期处理（Poll 内部也会做，这里让日志更清晰）

	// 接管前再追加一条 offset2，验证游标扫过死信后能继续首发。
	if off, err := b.Produce(tp, mq.Message{Value: []byte("fresh")}); err != nil || off != 2 {
		t.Fatalf("produce offset2: off=%d err=%v", off, err)
	}

	// 同组另一个消费者在故障检测后加入并接管（c1 已不在成员表）。
	c2, _ := b.JoinGroup("g", "c2")

	got := pollAll(t, b, c2)
	if len(got) != 2 {
		t.Fatalf("接管后投递数=%d want 2（offset0 回收重投 + offset2 首发）: %+v", len(got), got)
	}
	if got[0].Record.Offset != 0 || got[1].Record.Offset != 2 {
		t.Fatalf("接管后投递位移异常: %s,%s（死信位移 1 被复活）",
			got[0].Record.MessageID(), got[1].Record.MessageID())
	}
	for _, d := range got {
		if d.ConsumerID != "c2" {
			t.Fatalf("接管后投递必须投给新主 c2，实际 %s: %+v", d.ConsumerID, d)
		}
		if d.Attempt != 1 {
			t.Fatalf("%s 接管后应为新投递 attempt=1，实际 %d", d.Record.MessageID(), d.Attempt)
		}
	}
	t.Logf("判定: 接管后 %s 重投给 c2、%s 首发，死信 t-0#1 未被再投",
		got[0].Record.MessageID(), got[1].Record.MessageID())

	// 独占/租约栅栏不回归：旧主 c1 对其在途 offset0 的迟到确认必须被拒绝。
	if err := b.Ack(c1, first[0]); err == nil {
		t.Fatal("旧主 c1 过期后的迟到 Ack 竟然成功")
	} else {
		t.Logf("判定: 旧主 c1 接管后的迟到确认被拒绝（%v），无双持", err)
	}

	// 新主确认后提交位越过死信推进到日志末尾 3，不被死信卡住。
	for _, d := range got {
		if err := b.Ack(c2, d); err != nil {
			t.Fatalf("新主 ack %s: %v", d.Record.MessageID(), err)
		}
	}
	if err := b.CommitOffset(c2, tp, 3); err != nil {
		t.Fatalf("提交位应越过死信推进到 3: %v", err)
	}
	if more, _ := b.Poll(c2, 8); len(more) != 0 {
		t.Fatalf("全部确认后仍有投递: %+v", more)
	}

	dlq := b.DeadLetters("g")
	if len(dlq) != 1 || dlq[0].Offset != 1 {
		t.Fatalf("接管消费后死信应仍只有 1 条且指向 offset1，实际: %+v", dlq)
	}
	t.Log("判定: 租约过期接管后死信位移保持终态、不重复入死信；在途回收与栅栏语义不回归")
}

// TestReplayConcurrentTriggerAndQuery 验证多个调用方并发触发/续跑重投，
// 同时并发查询单个操作状态、列出组内操作时：
//   - 在 -race 下无数据竞争；
//   - 查询结果始终是自洽快照（status 与 items 状态一致、NewOffset 合法），
//     不会读到条目更新一半的中间态；
//   - 只有一个操作、一条重投副本（触发幂等 + 串行推进不被并发读破坏）。
func TestReplayConcurrentTriggerAndQuery(t *testing.T) {
	clk := &fakeClock{t: time.Unix(23000, 0)}
	cfg := retryCfg(t, clk)
	cfg.LeaseTTL = time.Hour
	cfg.PartCapacity = 20
	cfg = withLogger(cfg)
	tp := mq.TopicPartition{Topic: "t", Partition: 0}

	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	b.CreateTopic("t", 1)
	c, _ := b.JoinGroup("g", "c")

	// 造 3 条死信（offset 0/1/2）。
	for i := 0; i < 3; i++ {
		b.Produce(tp, mq.Message{Value: []byte("d" + strconv.Itoa(i))})
		killToDLQ(t, b, c, clk, "boom")
	}

	const writers, readers = 6, 6
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 校验一个快照自洽：要么 pending（允许部分完成）、要么全部 complete；
	// 已完成条目的 NewOffset 必须 >=0，未完成必须为 -1，不能出现撕裂值。
	checkSnapshot := func(op mq.ReplayOperation) {
		t.Helper()
		n := len(op.Items)
		if n != 3 {
			t.Errorf("快照条目数=%d want 3", n)
			return
		}
		complete := 0
		for idx, it := range op.Items {
			switch it.Status {
			case mq.ReplayComplete:
				complete++
				if it.NewOffset < 0 {
					t.Errorf("完成条目 %d 的 NewOffset=%d 非法", idx, it.NewOffset)
				}
			case mq.ReplayPending:
				// 允许 pending（反压/进行中），但不能有“完成状态却负位移”的撕裂。
			default:
				t.Errorf("条目 %d 出现非法状态 %q", idx, it.Status)
			}
		}
		switch op.Status {
		case mq.ReplayComplete:
			if complete != n {
				t.Errorf("操作标记 complete 但只有 %d/%d 条目完成（中间态泄露）", complete, n)
			}
		case mq.ReplayPending:
			if complete == n {
				t.Errorf("全部条目已完成但操作仍为 pending（中间态泄露）")
			}
		default:
			t.Errorf("操作出现非法状态 %q", op.Status)
		}
	}

	// 写者：并发用同一幂等键触发（首个建立操作），其余等价续跑。
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := b.ReplayDeadLetters("g", "concurrent-query-key", mq.ReplayFilter{})
				if err != nil && !errors.Is(err, mq.ErrNothingToReplay) {
					t.Errorf("并发触发 err=%v", err)
					return
				}
				checkSnapshot(res.Operation)
			}
		}()
	}
	// 读者 1：并发 GetReplay（操作建立前可能 ErrReplayNotFound，属正常）。
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 首个读者先用一次触发拿到 opID，供其余读者查询。
			res, err := b.ReplayDeadLetters("g", "concurrent-query-key", mq.ReplayFilter{})
			if err != nil && !errors.Is(err, mq.ErrNothingToReplay) {
				t.Errorf("读者引导触发 err=%v", err)
				return
			}
			opID := res.Operation.OpID
			for {
				select {
				case <-stop:
					return
				default:
				}
				op, err := b.GetReplay(opID)
				if err == nil {
					checkSnapshot(op)
				} else if !errors.Is(err, mq.ErrReplayNotFound) {
					t.Errorf("GetReplay err=%v", err)
					return
				}
			}
		}(i)
	}
	// 读者 2：并发 ListReplays，结果同样必须是自洽快照。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, op := range b.ListReplays("g") {
				checkSnapshot(op)
			}
		}
	}()

	// 让并发持续一小段时间后停止。
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	// 收敛：最终操作完成，且组内只有这一个操作。
	res, err := b.ReplayDeadLetters("g", "concurrent-query-key", mq.ReplayFilter{})
	if err != nil {
		t.Fatalf("最终收敛触发: %v", err)
	}
	if res.Operation.Status != mq.ReplayComplete || len(res.Operation.Items) != 3 {
		t.Fatalf("最终操作异常: %+v", res.Operation)
	}
	if ops := b.ListReplays("g"); len(ops) != 1 {
		t.Fatalf("应只有 1 个操作，实际 %d: %+v", len(ops), ops)
	}

	// 消费侧：三条重投副本恰好各一条，无重复入队。
	got := pollAll(t, b, c)
	if len(got) != 3 {
		t.Fatalf("重投副本数=%d want 3: %+v", len(got), got)
	}
	for i, d := range got {
		if d.Record.Offset != int64(3+i) {
			t.Fatalf("第 %d 条重投位移=%d want %d", i, d.Record.Offset, 3+i)
		}
	}
	t.Log("判定: 6 写者并发触发/续跑 + 多读者并发 Get/List，-race 下无竞争、快照始终自洽，仅 1 操作 3 副本")
}
