package mq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// joinAll registers every member sequentially and returns a consistent
// snapshot of the resulting generation and per-member assignments.
func joinAll(t *testing.T, b *Broker, groupID string, members []string) (int64, map[string][]TopicPartition) {
	t.Helper()
	for _, m := range members {
		if _, _, err := b.JoinGroup(groupID, m); err != nil {
			t.Fatalf("join %s: %v", m, err)
		}
	}
	am, gen, err := b.coord.assignment(groupID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]TopicPartition{}
	for tp, owner := range am {
		out[owner] = append(out[owner], tp)
	}
	for m := range out {
		sortTPs(out[m])
	}
	return gen, out
}

func TestGroupExclusiveAssignmentAndRebalance(t *testing.T) {
	b := mustOpen(t, testConfigReal(t))
	gen, assigns := joinAll(t, b, "g", []string{"a", "b"})
	t.Logf("ASSIGN gen=%d a=%v b=%v criterion=round-robin-exclusive", gen, assigns["a"], assigns["b"])
	if len(assigns["a"])+len(assigns["b"]) != 4 {
		t.Fatalf("partitions not fully assigned: %+v", assigns)
	}
	seen := map[TopicPartition]string{}
	for m, tps := range assigns {
		for _, tp := range tps {
			if other, dup := seen[tp]; dup {
				t.Fatalf("tp %s assigned to both %s and %s", tp, other, m)
			}
			seen[tp] = m
		}
	}
	if len(assigns["a"]) != 2 || len(assigns["b"]) != 2 {
		t.Fatalf("unbalanced: %+v", assigns)
	}

	// A non-owner Ack must be rejected.
	tp := assigns["a"][0]
	err := b.Ack("g", "b", gen, Delivered{Topic: tp.Topic, Partition: tp.Partition, Message: Message{Offset: 0}})
	if !errors.Is(err, ErrPartitionNotOwned) {
		t.Fatalf("non-owner ack: want ErrPartitionNotOwned, got %v", err)
	}
	t.Logf("OWNER tp=%s actor=b owner=a criterion=non-owner-ack-rejected", tp)
}

func TestStaleGenerationRejected(t *testing.T) {
	b := mustOpen(t, testConfigReal(t))
	genA, _, _ := b.JoinGroup("g", "a")
	// A second member joining forces a rebalance -> new generation.
	genB, _, _ := b.JoinGroup("g", "b")
	if genB <= genA {
		t.Fatalf("generation did not advance: %d -> %d", genA, genB)
	}
	if _, _, err := b.Produce(context.Background(), "orders", Message{Key: "x"}); err != nil {
		t.Fatal(err)
	}
	am, _, _ := b.coord.assignment("g")
	var bTP TopicPartition
	for tp, owner := range am {
		if owner == "b" {
			bTP = tp
			break
		}
	}
	_, err := b.Fetch(context.Background(), "g", "a", genA, bTP)
	if !errors.Is(err, ErrStaleGeneration) && !errors.Is(err, ErrPartitionNotOwned) {
		t.Fatalf("stale fetch: want ErrStaleGeneration/NotOwned, got %v", err)
	}
	t.Logf("GEN old=%d current=%d tp=%s criterion=stale-generation-or-nonowner-rejected", genA, genB, bTP)
}

func TestConcurrentConsumersExclusiveProcessing(t *testing.T) {
	cfg := testConfigReal(t)
	cfg.Partitions = 3
	cfg.LeaseTTL = 5 * time.Second
	b := mustOpen(t, cfg)
	ctx := context.Background()
	for i := 0; i < 60; i++ {
		if _, _, err := b.Produce(ctx, "orders", Message{Body: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}

	const n = 3
	for i := 0; i < n; i++ {
		if _, _, err := b.JoinGroup("grp", string(rune('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	am, gen, _ := b.coord.assignment("grp")
	t.Logf("CONCURRENT gen=%d assignment=%v", gen, am)

	var processed int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		mid := string(rune('A' + i))
		var mine []TopicPartition
		for tp, owner := range am {
			if owner == mid {
				mine = append(mine, tp)
			}
		}
		sortTPs(mine)
		wg.Add(1)
		go func(mid string, mine []TopicPartition) {
			defer wg.Done()
			local := map[TopicPartition]int64{}
			for i := 0; i < 20; i++ {
				for _, tp := range mine {
					d, err := b.Fetch(ctx, "grp", mid, gen, tp)
					if err != nil {
						return
					}
					if prev, ok := local[tp]; ok && d.Message.Offset != prev+1 {
						panic("out-of-order")
					}
					local[tp] = d.Message.Offset
					if err := b.Ack("grp", mid, gen, d); err != nil {
						panic(err)
					}
					atomic.AddInt64(&processed, 1)
				}
			}
		}(mid, mine)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("concurrent consumers deadlocked, processed=%d", atomic.LoadInt64(&processed))
	}
	if got := atomic.LoadInt64(&processed); got != 60 {
		t.Fatalf("processed = %d want 60 (no loss, no dup, no double ownership)", got)
	}
	t.Logf("CONCURRENT processed=60 criterion=exclusive-ordered-no-loss-no-dup")
}
