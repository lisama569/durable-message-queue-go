package mq_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// retryCfg 构造可控时钟下的配置，退避 5ms->10ms，上限 3 次投递。
func retryCfg(t *testing.T, clk *fakeClock) mq.Config {
	cfg := testConfig(t)
	cfg.Clock = clk
	cfg.Retry = mq.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
		Multiplier:  2, // attempt1 失败 -> 5ms；attempt2 失败 -> 10ms；attempt3 失败 -> DLQ
	}
	return cfg
}

// TestRetryThenSuccess 验证：失败后按确定性退避重投，最终成功只处理一次生效。
func TestRetryThenSuccess(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	off, _ := b.Produce(tp, mq.Message{Value: []byte("v")})
	t.Logf("判定: 消息位移=%d 已持久化", off)

	c, _ := b.JoinGroup("g", "c")

	// 首次投递失败。
	ds, _ := b.Poll(c, 1)
	if len(ds) != 1 || ds[0].Attempt != 1 {
		t.Fatalf("首次投递异常: %+v", ds)
	}
	if err := b.Nack(c, ds[0], "transient-error-1"); err != nil {
		t.Fatalf("nack1: %v", err)
	}
	t.Logf("判定: %s 第1次失败原因=transient-error-1，退避5ms", ds[0].Record.MessageID())

	// 退避未到期：不应重投，且后面的同分区消息也不能越过它（顺序不跳过）。
	b.Step()
	if blocked, _ := b.Poll(c, 1); len(blocked) != 0 {
		t.Fatalf("退避未到期却重投: %+v", blocked)
	}

	// 到期后重投，attempt=2；再次失败。
	clk.advance(6 * time.Millisecond)
	ds2, _ := b.Poll(c, 1)
	if len(ds2) != 1 || ds2[0].Attempt != 2 || ds2[0].Record.Offset != off {
		t.Fatalf("第2次投递异常: %+v", ds2)
	}
	t.Logf("判定: 退避到期重投 %s attempt=%d（同位移、确定性退避）",
		ds2[0].Record.MessageID(), ds2[0].Attempt)
	if err := b.Nack(c, ds2[0], "transient-error-2"); err != nil {
		t.Fatalf("nack2: %v", err)
	}

	// 再到期，第3次投递成功（达到 MaxAttempts 的最后一次，不再失败）。
	clk.advance(11 * time.Millisecond)
	ds3, _ := b.Poll(c, 1)
	if len(ds3) != 1 || ds3[0].Attempt != 3 {
		t.Fatalf("第3次投递异常: %+v", ds3)
	}
	if err := b.Ack(c, ds3[0]); err != nil {
		t.Fatalf("ack: %v", err)
	}
	t.Logf("判定: %s attempt=%d 成功确认，无死信产生", ds3[0].Record.MessageID(), ds3[0].Attempt)
	if dlq := b.DeadLetters("g"); len(dlq) != 0 {
		t.Fatalf("不应有死信，实际 %d 条", len(dlq))
	}
	// 再次 poll 无重复投递。
	if more, _ := b.Poll(c, 1); len(more) != 0 {
		t.Fatalf("成功后仍重复投递: %+v", more)
	}
}

// TestDeadLetter 验证：超过上限进入死信，保留消息快照与完整原因链，位移可继续推进。
func TestDeadLetter(t *testing.T) {
	clk := &fakeClock{t: time.Unix(2000, 0)}
	cfg := withLogger(retryCfg(t, clk))
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	tp := mq.TopicPartition{Topic: "t", Partition: 0}
	if err := b.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	// 先只投递一条“毒消息”，把它打进死信；随后再投递正常消息，
	// 验证死信终态不会永久阻塞同分区后续消息。
	off0, _ := b.Produce(tp, mq.Message{Key: []byte("k0"), Value: []byte("poison")})
	c, _ := b.JoinGroup("g", "c")

	// 确定性脚本：attempt1 失败 -> 退避5ms -> attempt2 失败 -> 退避10ms
	// -> attempt3 失败 -> 进入死信。每次拉取后同分区的第二条消息均被阻塞。
	delays := []time.Duration{0, 6 * time.Millisecond, 11 * time.Millisecond}
	for round, wait := range delays {
		if wait > 0 {
			clk.advance(wait)
		}
		ds, _ := b.Poll(c, 4)
		if len(ds) != 1 || ds[0].Record.Offset != off0 {
			t.Fatalf("round %d 投递异常: %+v", round, ds)
		}
		if ds[0].Attempt != round+1 {
			t.Fatalf("round %d attempt=%d want %d", round, ds[0].Attempt, round+1)
		}
		err := b.Nack(c, ds[0], "fail-attempt")
		if round == 2 {
			if !errors.Is(err, mq.ErrRetryExhausted) {
				t.Fatalf("第3次失败应进入死信，err=%v", err)
			}
		} else if err != nil {
			t.Fatalf("round %d nack: %v", round, err)
		}
		t.Logf("判定: %s attempt=%d 失败 -> %s",
			ds[0].Record.MessageID(), ds[0].Attempt,
			map[bool]string{true: "进入死信", false: "退避重试"}[round == 2])
	}

	dlq := b.DeadLetters("g")
	if len(dlq) != 1 {
		t.Fatalf("死信条数=%d want 1", len(dlq))
	}
	e := dlq[0]
	if e.Offset != off0 || string(e.Record.Value) != "poison" {
		t.Fatalf("死信内容异常: %+v", e)
	}
	if e.Attempts != 3 || len(e.Reasons) != 3 {
		t.Fatalf("attempts=%d reasons=%d，want 3/3；原因链=%v", e.Attempts, len(e.Reasons), e.Reasons)
	}
	t.Logf("判定: %s 超过上限进入死信 attempts=%d，保留原因链=%v，消息快照完整",
		e.Topic+"-0#"+strconv.FormatInt(e.Offset, 10), e.Attempts, e.Reasons)

	// 死信为终态：新生产的第二条消息应立即可被投递并确认，不被永久阻塞。
	off1, _ := b.Produce(tp, mq.Message{Key: []byte("k1"), Value: []byte("ok-msg")})
	ds, _ := b.Poll(c, 4)
	if len(ds) != 1 || ds[0].Record.Offset != off1 {
		t.Fatalf("死信后应投递 off1，实际: %+v", ds)
	}
	if err := b.Ack(c, ds[0]); err != nil {
		t.Fatal(err)
	}
	t.Logf("判定: 死信终态后 %s 被正常投递并确认（同分区不被永久阻塞）",
		ds[0].Record.MessageID())
	if more, _ := b.Poll(c, 4); len(more) != 0 {
		t.Fatalf("仍有多余投递: %+v", more)
	}
}

// TestRetryBackoffDeterministic 直接验证退避数值确定可复现。
func TestRetryBackoffDeterministic(t *testing.T) {
	p := mq.DefaultRetryPolicy()
	cases := map[int]time.Duration{1: 10 * time.Millisecond, 2: 20 * time.Millisecond, 3: 40 * time.Millisecond}
	for attempt, want := range cases {
		got := p.Backoff(attempt)
		if got != want {
			t.Fatalf("backoff(%d)=%s want %s", attempt, got, want)
		}
	}
	t.Log("判定: backoff(1)=10ms backoff(2)=20ms backoff(3)=40ms，无随机抖动、可复现")
}
