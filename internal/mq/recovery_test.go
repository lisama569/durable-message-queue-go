package mq

import (
	"context"
	"testing"
	"time"
)

// Commit is idempotent and fetch never re-returns an acknowledged message:
// after Ack, the next fetch yields the following offset.
func TestAckedMessageNotRedeliveredButDuplicateAckIsSafe(t *testing.T) {
	b := mustOpen(t, testConfigReal(t))
	tp := produceN(t, b, "key", 3)
	gen, _, _ := b.JoinGroup("g", "c")
	ctx := context.Background()

	d, err := b.Fetch(ctx, "g", "c", gen, tp)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("g", "c", gen, d); err != nil {
		t.Fatal(err)
	}
	// Re-acking the same delivery must be safe (idempotent commit).
	if err := b.Ack("g", "c", gen, d); err != nil {
		t.Fatalf("duplicate ack should be idempotent: %v", err)
	}
	t.Logf("OFFSET tp=%s offset=%d committed=1 criterion=duplicate-ack-idempotent", tp, d.Message.Offset)

	d2, err := b.Fetch(ctx, "g", "c", gen, tp)
	if err != nil {
		t.Fatal(err)
	}
	if d2.Message.Offset != 1 {
		t.Fatalf("acked offset 0 must not be redelivered; got %d", d2.Message.Offset)
	}
	t.Logf("DELIVER tp=%s offset=%d criterion=acked-message-not-returned-again", tp, d2.Message.Offset)
}

// Commit progress before a restart survives; after restart the group resumes
// exactly at the committed offset and consumes the remaining messages once.
func TestRestartResumesAtCommittedOffset(t *testing.T) {
	cfg := testConfigReal(t)
	cfg.LeaseTTL = time.Hour
	b := mustOpen(t, cfg)
	tp := produceN(t, b, "key", 5)
	gen, _, _ := b.JoinGroup("g", "c")
	ctx := context.Background()
	// Consume and ack first 2, then "crash" before touching the rest.
	for i := 0; i < 2; i++ {
		d, err := b.Fetch(ctx, "g", "c", gen, tp)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Ack("g", "c", gen, d); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	gen2, _, _ := b2.JoinGroup("g", "c")
	var got []int64
	for i := 0; i < 3; i++ {
		d, err := b2.Fetch(ctx, "g", "c", gen2, tp)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("DELIVER(post-restart) tp=%s offset=%d body=%q criterion=resume-at-committed-2",
			tp, d.Message.Offset, string(d.Message.Body))
		got = append(got, d.Message.Offset)
		if err := b2.Ack("g", "c", gen2, d); err != nil {
			t.Fatal(err)
		}
	}
	want := []int64{2, 3, 4}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("post-restart offsets = %v want %v (no skip, no dup)", got, want)
		}
	}
	st, _ := b2.PartitionStats(tp.Topic, tp.Partition)
	if c := st.Committed["g"]; c != 5 {
		t.Fatalf("final committed = %d want 5", c)
	}
	t.Logf("RESTART tp=%s final-committed=5 lag=%d criterion=consumed-remaining-once", tp, st.Lag)
}
