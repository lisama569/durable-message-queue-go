package mq

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestOffsetCommitValidation(t *testing.T) {
	s, err := openOffsetStore(filepath.Join(t.TempDir(), "offsets.json"))
	if err != nil {
		t.Fatal(err)
	}
	g, tp := "g", "t"
	logEnd := int64(5)

	// Initial forward commit.
	if err := s.commit(g, tp, 0, 3, logEnd); err != nil {
		t.Fatalf("commit 3: %v", err)
	}
	t.Logf("OFFSET group=g topic=t partition=0 committed=3 criterion=forward-commit-accepted")

	// Idempotent repeat: no-op success.
	if err := s.commit(g, tp, 0, 3, logEnd); err != nil {
		t.Fatalf("repeat commit should be idempotent: %v", err)
	}
	t.Logf("OFFSET offset=3 committed=3 criterion=duplicate-commit-idempotent-no-op")

	// Regression must be rejected with a distinguishable reason.
	err = s.commit(g, tp, 0, 2, logEnd)
	var oe *OffsetError
	if !errors.As(err, &oe) || oe.Reason != OffsetRejectRegress {
		t.Fatalf("regression: want OffsetRejectRegress, got %v", err)
	}
	if got := s.get(g, tp, 0); got != 3 {
		t.Fatalf("state mutated after rejection: %d", got)
	}
	t.Logf("OFFSET offset=2 committed=3 reason=%s criterion=regression-rejected-state-unchanged", oe.Reason)

	// Out-of-range above log end.
	err = s.commit(g, tp, 0, 6, logEnd)
	if !errors.As(err, &oe) || oe.Reason != OffsetRejectOutOfRange {
		t.Fatalf("out of range: want OffsetRejectOutOfRange, got %v", err)
	}
	t.Logf("OFFSET offset=6 logEnd=5 reason=%s criterion=out-of-range-rejected", oe.Reason)

	// Negative offset.
	err = s.commit(g, tp, 0, -1, logEnd)
	if !errors.As(err, &oe) || oe.Reason != OffsetRejectOutOfRange {
		t.Fatalf("negative: want OffsetRejectOutOfRange, got %v", err)
	}
}

func TestOffsetPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.json")
	s, _ := openOffsetStore(path)
	if err := s.commit("g", "t", 0, 4, 10); err != nil {
		t.Fatal(err)
	}
	s2, err := openOffsetStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.get("g", "t", 0); got != 4 {
		t.Fatalf("reloaded committed = %d want 4", got)
	}
	t.Logf("RESTART committed=4 criterion=offset-survives-restart")
}
