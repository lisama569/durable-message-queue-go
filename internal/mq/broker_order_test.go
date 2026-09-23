package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// produceN appends n messages with the same key; they must land on one
// partition with stable offsets 0..n-1.
func produceN(t *testing.T, b *Broker, key string, n int) TopicPartition {
	t.Helper()
	ctx := context.Background()
	var tp TopicPartition
	for i := 0; i < n; i++ {
		got, off, err := b.Produce(ctx, "orders", Message{Key: key, Body: []byte("m" + itoa(i))})
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		if i == 0 {
			tp = got
		} else if got != tp {
			t.Fatalf("key %s changed partition: %v -> %v", key, tp, got)
		}
		if off != int64(i) {
			t.Fatalf("offset for msg %d = %d want %d", i, off, i)
		}
	}
	return tp
}

func TestOrderedDeliveryForKeyAndAckCommit(t *testing.T) {
	b := mustOpen(t, testConfig(t))
	tp := produceN(t, b, "user-1", 5)

	gen, tps, err := b.JoinGroup("g1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JOIN group=g1 member=c1 gen=%d partitions=%v criterion=registered", gen, tps)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		d, err := b.Fetch(ctx, "g1", "c1", gen, tp)
		if err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
		if d.Message.Offset != int64(i) {
			t.Fatalf("delivery order broken: got offset %d want %d", d.Message.Offset, i)
		}
		t.Logf("DELIVER group=g1 tp=%s offset=%d delivery=%d body=%q criterion=in-offset-order",
			tp, d.Message.Offset, d.Delivery, string(d.Message.Body))
		if err := b.Ack("g1", "c1", gen, d); err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
		st, _ := b.PartitionStats(tp.Topic, tp.Partition)
		t.Logf("OFFSET tp=%s offset=%d committed=%d criterion=ack-commits-next-offset",
			tp, i, st.Committed["g1"])
		if st.Committed["g1"] != int64(i+1) {
			t.Fatalf("committed = %d want %d", st.Committed["g1"], i+1)
		}
	}
}

func TestCommitOffsetRegressionAndOutOfRange(t *testing.T) {
	b := mustOpen(t, testConfig(t))
	tp := produceN(t, b, "user-2", 3)
	gen, _, _ := b.JoinGroup("g2", "c1")

	if err := b.CommitOffset("g2", "c1", gen, tp, 2); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	t.Logf("OFFSET tp=%s committed=2 criterion=explicit-forward-commit", tp)

	err := b.CommitOffset("g2", "c1", gen, tp, 1)
	var oe *OffsetError
	if !errors.As(err, &oe) || oe.Reason != OffsetRejectRegress {
		t.Fatalf("want regression error, got %v", err)
	}
	t.Logf("OFFSET tp=%s attempted=1 committed=2 reason=%s criterion=regression-rejected",
		tp, oe.Reason)

	err = b.CommitOffset("g2", "c1", gen, tp, 99)
	if !errors.As(err, &oe) || oe.Reason != OffsetRejectOutOfRange {
		t.Fatalf("want out-of-range error, got %v", err)
	}
	t.Logf("OFFSET tp=%s attempted=99 logEnd=3 reason=%s criterion=out-of-range-rejected",
		tp, oe.Reason)

	// Same commit twice is idempotent.
	if err := b.CommitOffset("g2", "c1", gen, tp, 2); err != nil {
		t.Fatalf("idempotent commit failed: %v", err)
	}
	t.Logf("OFFSET tp=%s committed=2 criterion=duplicate-commit-idempotent", tp)
}

func TestFetchBlocksAndWakesOnProduce(t *testing.T) {
	b := mustOpen(t, testConfig(t))
	gen, tps, _ := b.JoinGroup("g3", "c1")
	tp := tps[0]

	got := make(chan Delivered, 1)
	go func() {
		d, err := b.Fetch(context.Background(), "g3", "c1", gen, tp)
		if err == nil {
			got <- d
		}
	}()
	time.Sleep(30 * time.Millisecond)
	if _, _, err := b.Produce(context.Background(), tp.Topic, Message{Body: []byte("late")}); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		t.Logf("DELIVER tp=%s offset=%d body=%q criterion=blocking-fetch-woken-by-produce",
			tp, d.Message.Offset, string(d.Message.Body))
	case <-time.After(time.Second):
		t.Fatal("fetch was not woken by produce")
	}
}
