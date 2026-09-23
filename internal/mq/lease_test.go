package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHeartbeatReapsExpiredMemberAndWakesBlockedFetch(t *testing.T) {
	cfg := testConfigReal(t)
	cfg.Partitions = 1
	cfg.LeaseTTL = 80 * time.Millisecond
	b := mustOpen(t, cfg)
	ctx := context.Background()

	// a joins and starts a blocking Fetch on the only partition.
	genA, tpsA, _ := b.JoinGroup("g", "a")
	fetchErr := make(chan error, 1)
	go func() {
		_, err := b.Fetch(ctx, "g", "a", genA, tpsA[0])
		fetchErr <- err
	}()

	// b joins (gets nothing, the single partition is a's), then keeps
	// heartbeating. After a falls silent, b's heartbeat reaps a and takes
	// the partition without a ever rejoining.
	genB, _, _ := b.JoinGroup("g", "b")
	time.Sleep(120 * time.Millisecond)
	curGen, err := b.Heartbeat("g", "b")
	if err != nil {
		t.Fatal(err)
	}
	if curGen <= genB {
		t.Fatalf("generation should advance on reap: %d -> %d", genB, curGen)
	}
	am, gen, _ := b.coord.assignment("g")
	if am[tpsA[0]] != "b" {
		t.Fatalf("partition should move to b after heartbeat-reap, owner=%q", am[tpsA[0]])
	}
	t.Logf("REAP gen=%d tp=%s -> b criterion=heartbeat-reaps-expired-member", gen, tpsA[0])

	// a's blocked fetch must have been woken and now fail (no silent stall).
	select {
	case e := <-fetchErr:
		if e == nil {
			t.Fatal("expired member's fetch should fail after partition revocation")
		}
		if !errors.Is(e, ErrLeaseExpired) && !errors.Is(e, ErrUnknownMember) &&
			!errors.Is(e, ErrStaleGeneration) && !errors.Is(e, ErrPartitionNotOwned) {
			t.Fatalf("unexpected fetch error: %v", e)
		}
		t.Logf("REVOKE member=a fetch-error=%q criterion=blocked-fetch-woken-and-rejected", e)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked fetch of expired member was not woken")
	}
}
