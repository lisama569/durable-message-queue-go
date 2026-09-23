package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNackRetriesWithDeterministicBackoffThenDeadLetter(t *testing.T) {
	cfg := testConfig(t)
	clk := newFakeClock()
	cfg.Now = clk.now
	cfg.MaxRetries = 2
	cfg.LeaseTTL = time.Hour // fake clock is advanced manually; keep lease live
	cfg.Backoff = ConstantBackoff{D: 50 * time.Millisecond}
	b := mustOpen(t, cfg)

	tp, off0, err := b.Produce(context.Background(), "orders", Message{Key: "k", Body: []byte("job")})
	if err != nil || off0 != 0 {
		t.Fatalf("produce: %v off=%d", err, off0)
	}
	gen, _, _ := b.JoinGroup("g", "w1")

	// Attempt 1 -> first delivery.
	d1, err := b.Fetch(context.Background(), "g", "w1", gen, tp)
	if err != nil {
		t.Fatal(err)
	}
	if d1.Delivery != 1 {
		t.Fatalf("delivery = %d want 1", d1.Delivery)
	}
	t.Logf("DELIVER tp=%s offset=0 delivery=1 criterion=first-delivery", tp)

	// Nack: attempt 1 <= MaxRetries(2) -> retry scheduled at +50ms.
	if err := b.Nack("g", "w1", gen, d1, "boom-1"); err != nil {
		t.Fatal(err)
	}
	st, _ := b.PartitionStats(tp.Topic, tp.Partition)
	t.Logf("RETRY tp=%s offset=0 attempts=1 retryPending=%d notBefore=%s criterion=backoff-scheduled",
		tp, st.RetryPending, clk.now().Add(50*time.Millisecond).Format(time.RFC3339Nano))
	if st.RetryPending != 1 {
		t.Fatalf("retry pending = %d want 1", st.RetryPending)
	}

	// Immediately re-fetch: head-of-line must block; advancing only 49ms
	// keeps the message unavailable.
	clk.advance(49 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, ferr := b.Fetch(ctx, "g", "w1", gen, tp)
	cancel()
	if !errors.Is(ferr, context.DeadlineExceeded) {
		t.Fatalf("fetch during backoff should time out, got %v", ferr)
	}
	t.Logf("RETRY tp=%s offset=0 elapsed=49ms backoff=50ms criterion=head-of-line-blocked-not-skipped", tp)

	// Advance past backoff -> redelivery attempt 2.
	clk.advance(2 * time.Millisecond)
	d2, err := b.Fetch(context.Background(), "g", "w1", gen, tp)
	if err != nil {
		t.Fatal(err)
	}
	if d2.Delivery != 2 {
		t.Fatalf("delivery = %d want 2", d2.Delivery)
	}
	t.Logf("DELIVER tp=%s offset=0 delivery=2 criterion=redelivery-after-exact-backoff", tp)
	if err := b.Nack("g", "w1", gen, d2, "boom-2"); err != nil {
		t.Fatal(err)
	}

	// Third failure: attempts(3) > MaxRetries(2) -> dead letter, offset advances.
	// attempt 2 also scheduled a 50ms backoff, so advance the clock past it.
	clk.advance(50 * time.Millisecond)
	d3, err := b.Fetch(context.Background(), "g", "w1", gen, tp)
	if err != nil {
		t.Fatal(err)
	}
	if d3.Delivery != 3 {
		t.Fatalf("delivery = %d want 3", d3.Delivery)
	}
	if err := b.Nack("g", "w1", gen, d3, "boom-3-final"); err != nil {
		t.Fatal(err)
	}
	dls := b.DeadLetters()
	if len(dls) != 1 {
		t.Fatalf("dlq len = %d want 1", len(dls))
	}
	dl := dls[0]
	if dl.Reason != "boom-3-final" || dl.Attempts != 3 || dl.Offset != 0 {
		t.Fatalf("dead letter wrong: %+v", dl)
	}
	t.Logf("DLQ tp=%s offset=%d attempts=%d reason=%q criterion=exhausted-retries-original-reason-kept",
		tp, dl.Offset, dl.Attempts, dl.Reason)

	// The committed offset must have advanced past the poison message so
	// the partition is not stuck.
	st, _ = b.PartitionStats(tp.Topic, tp.Partition)
	if c := st.Committed["g"]; c != 1 {
		t.Fatalf("committed after DLQ = %d want 1", c)
	}
	t.Logf("OFFSET tp=%s committed=1 criterion=dead-letter-advances-offset-no-stuck-partition", tp)
}

func TestBackoffScheduleIsDeterministic(t *testing.T) {
	lin := LinearBackoff{Base: 10 * time.Millisecond}
	want := []time.Duration{10, 20, 30, 40}
	for i, w := range want {
		if got := lin.Delay(i + 1); got != w*time.Millisecond {
			t.Fatalf("linear delay %d = %v want %v", i+1, got, w)
		}
	}
	con := ConstantBackoff{D: 7 * time.Millisecond}
	for a := 1; a <= 4; a++ {
		if got := con.Delay(a); got != 7*time.Millisecond {
			t.Fatalf("constant delay %d = %v want 7ms", a, got)
		}
	}
	t.Logf("BACKOFF linear=[10,20,30,40]ms constant=7ms criterion=no-randomness-reproducible")
}
