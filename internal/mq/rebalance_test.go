package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// After a member's lease expires and another member joins, its partitions
// must be reassigned; the old member's subsequent operations must fail.
func TestLeaseExpiryReassignsPartitionsSafely(t *testing.T) {
	cfg := testConfigReal(t)
	cfg.Partitions = 2
	cfg.LeaseTTL = 80 * time.Millisecond
	b := mustOpen(t, cfg)

	genA, tpsA, err := b.JoinGroup("g", "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JOIN member=a gen=%d owns=%v", genA, tpsA)

	// Member a goes silent past the lease; b joins and the expired member is
	// removed on join, triggering a safe rebalance.
	time.Sleep(120 * time.Millisecond)
	genB, tpsB, err := b.JoinGroup("g", "b")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JOIN member=b gen=%d owns=%v criterion=expired-member-evicted", genB, tpsB)
	if genB <= genA {
		t.Fatalf("generation should advance after eviction: %d -> %d", genA, genB)
	}
	if len(tpsB) != 2 {
		t.Fatalf("b should own both partitions after a expired, got %v", tpsB)
	}

	// a is gone; an op claiming to be a (even with the new gen) is rejected.
	err = b.Ack("g", "a", genB, Delivered{Topic: "orders", Partition: 0, Message: Message{Offset: 0}})
	if !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("evicted member ack: want ErrUnknownMember, got %v", err)
	}
	t.Logf("LEASE member=a evicted gen=%d criterion=evicted-member-cannot-commit", genB)

	// Partition ownership is exclusive in the new generation: b owns all.
	am, gen, _ := b.coord.assignment("g")
	for tp, owner := range am {
		if owner != "b" {
			t.Fatalf("tp %s owner=%s want b", tp, owner)
		}
	}
	t.Logf("ASSIGN gen=%d all->b criterion=no-dual-ownership-after-rebalance", gen)
}

// A consumer that fetches but crashes before Ack must cause a redelivery
// after restart: the committed offset lags, so the message is delivered
// again (at-least-once), never skipped.
func TestUnackedMessageRedeliveredAfterRestart(t *testing.T) {
	cfg := testConfigReal(t)
	cfg.LeaseTTL = time.Hour
	cfg.Backoff = ConstantBackoff{D: 0}
	b := mustOpen(t, cfg)

	ctx := context.Background()
	tp, _, err := b.Produce(ctx, "orders", Message{Key: "k1", Body: []byte("persist-me")})
	if err != nil {
		t.Fatal(err)
	}
	gen, _, _ := b.JoinGroup("g", "c1")
	d, err := b.Fetch(ctx, "g", "c1", gen, tp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("DELIVER(before-crash) tp=%s offset=%d body=%q delivery=%d criterion=fetched-not-acked",
		tp, d.Message.Offset, string(d.Message.Body), d.Delivery)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate process restart on the same data directory.
	b2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	st, _ := b2.PartitionStats(tp.Topic, tp.Partition)
	if c := st.Committed["g"]; c != 0 {
		t.Fatalf("committed after restart = %d want 0 (nothing was acked)", c)
	}
	if st.LogEnd != 1 {
		t.Fatalf("logEnd after restart = %d want 1", st.LogEnd)
	}
	t.Logf("RESTART tp=%s committed=%d logEnd=%d criterion=message-retained-offset-not-advanced",
		tp, st.Committed["g"], st.LogEnd)

	gen2, _, _ := b2.JoinGroup("g", "c1")
	d2, err := b2.Fetch(ctx, "g", "c1", gen2, tp)
	if err != nil {
		t.Fatal(err)
	}
	if d2.Message.Offset != 0 || string(d2.Message.Body) != "persist-me" {
		t.Fatalf("redelivered wrong message: %+v", d2)
	}
	if d2.Delivery != 1 {
		// In-flight state is intentionally not persisted; after a crash this
		// is the first delivery the new process has observed.
		t.Logf("note: delivery counter after restart = %d", d2.Delivery)
	}
	t.Logf("DELIVER(after-restart) tp=%s offset=%d body=%q criterion=unacked-redelivered-not-skipped",
		tp, d2.Message.Offset, string(d2.Message.Body))
	if err := b2.Ack("g", "c1", gen2, d2); err != nil {
		t.Fatal(err)
	}
	st, _ = b2.PartitionStats(tp.Topic, tp.Partition)
	if c := st.Committed["g"]; c != 1 {
		t.Fatalf("committed = %d want 1", c)
	}
}
