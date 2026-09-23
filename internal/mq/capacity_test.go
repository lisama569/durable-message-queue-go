package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCapacityBackpressureAndWatermarkStats(t *testing.T) {
	cfg := testConfigReal(t)
	cfg.Partitions = 1
	cfg.PartitionCap = 3
	cfg.HighWatermark = 2
	b := mustOpen(t, cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, _, err := b.Produce(ctx, "orders", Message{Body: []byte("x")}); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	st, _ := b.PartitionStats("orders", 0)
	if !st.Full {
		t.Fatalf("expected full, stats=%+v", st)
	}
	if !st.AtHighWatermark {
		t.Fatalf("expected at high watermark, stats=%+v", st)
	}
	t.Logf("WATERMARK logEnd=%d cap=%d wm=%d full=%v atWM=%v criterion=capacity-observable",
		st.LogEnd, 3, 2, st.Full, st.AtHighWatermark)

	// A 4th produce blocks under backpressure and does not silently drop.
	errCh := make(chan error, 1)
	go func() {
		_, _, err := b.Produce(ctx, "orders", Message{Body: []byte("waiting")})
		errCh <- err
	}()
	select {
	case err := <-errCh:
		t.Fatalf("produce should block at capacity, returned err=%v", err)
	case <-time.After(60 * time.Millisecond):
	}

	// A consumer drains one message; the blocked produce must then succeed
	// without losing the waiting message.
	gen, tps, _ := b.JoinGroup("g", "c")
	d, err := b.Fetch(ctx, "g", "c", gen, tps[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("g", "c", gen, d); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("blocked produce after drain: %v", err)
		}
		t.Logf("BACKPRESSURE drained=offset0 blocked-produce-resumed criterion=no-drop-no-dup")
	case <-time.After(2 * time.Second):
		t.Fatal("blocked produce was never unblocked by capacity freeing")
	}

	st, _ = b.PartitionStats("orders", 0)
	if st.LogEnd != 4 {
		t.Fatalf("logEnd = %d want 4 (3 + the formerly waiting message)", st.LogEnd)
	}
}

func TestProduceHonorsContextUnderBackpressure(t *testing.T) {
	cfg := testConfigReal(t)
	cfg.Partitions = 1
	cfg.PartitionCap = 1
	b := mustOpen(t, cfg)
	if _, _, err := b.Produce(context.Background(), "orders", Message{Body: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, _, err := b.Produce(ctx, "orders", Message{Body: []byte("b")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context deadline under backpressure, got %v", err)
	}
	st, _ := b.PartitionStats("orders", 0)
	if st.LogEnd != 1 {
		t.Fatalf("timed-out produce must not enqueue: logEnd=%d want 1", st.LogEnd)
	}
	t.Logf("BACKPRESSURE logEnd=1 after-timeout criterion=cancelled-produce-not-enqueued")
}
