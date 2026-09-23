package mq

import (
	"fmt"
	"sync"
)

// offsetStore persists committed offsets:
// offsets[group][topic][partition] = committed offset (next offset to consume).
type offsetStore struct {
	path    string
	mu      sync.Mutex
	offsets map[string]map[string]map[int]int64
}

func openOffsetStore(path string) (*offsetStore, error) {
	s := &offsetStore{
		path:    path,
		offsets: make(map[string]map[string]map[int]int64),
	}
	if err := readJSON(path, &s.offsets); err != nil {
		return nil, fmt.Errorf("mq: load offsets: %w", err)
	}
	if s.offsets == nil {
		s.offsets = make(map[string]map[string]map[int]int64)
	}
	return s, nil
}

func (s *offsetStore) get(group, topic string, partition int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(group, topic, partition)
}

func (s *offsetStore) getLocked(group, topic string, partition int) int64 {
	if t, ok := s.offsets[group]; ok {
		if p, ok := t[topic]; ok {
			return p[partition]
		}
	}
	return 0
}

// commit validates and stores a new committed offset.
//
//	offset == current : idempotent no-op (success)
//	offset <  current : rejected as OffsetRejectRegress
//	offset out of [0, logEnd]: rejected as OffsetRejectOutOfRange
//
// State is never mutated on rejection.
func (s *offsetStore) commit(group, topic string, partition int, offset, logEnd int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.getLocked(group, topic, partition)
	if err := validateOffset(group, topic, partition, offset, current, logEnd); err != nil {
		return err
	}
	if offset == current {
		return nil // idempotent
	}
	s.setLocked(group, topic, partition, offset)
	return s.persistLocked()
}

func validateOffset(group, topic string, partition int, offset, current, logEnd int64) *OffsetError {
	switch {
	case offset == current:
		return nil
	case offset < 0 || offset > logEnd:
		return &OffsetError{
			Topic: topic, Partition: partition, Offset: offset,
			Reason: OffsetRejectOutOfRange,
			Detail: fmt.Sprintf("valid range [0,%d]", logEnd),
		}
	case offset < current:
		return &OffsetError{
			Topic: topic, Partition: partition, Offset: offset,
			Reason: OffsetRejectRegress,
			Detail: fmt.Sprintf("committed=%d (cannot move backward)", current),
		}
	default:
		return nil
	}
}

func (s *offsetStore) setLocked(group, topic string, partition int, offset int64) {
	t, ok := s.offsets[group]
	if !ok {
		t = make(map[string]map[int]int64)
		s.offsets[group] = t
	}
	p, ok := t[topic]
	if !ok {
		p = make(map[int]int64)
		t[topic] = p
	}
	p[partition] = offset
}

// forceAdvance moves the committed offset strictly forward to offset.
// Used only by the dead-letter path; regressions are rejected here too.
func (s *offsetStore) forceAdvance(group, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.getLocked(group, topic, partition)
	if offset <= current {
		return nil // nothing to do
	}
	s.setLocked(group, topic, partition, offset)
	return s.persistLocked()
}

func (s *offsetStore) persistLocked() error {
	if err := writeJSONAtomically(s.path, s.offsets); err != nil {
		return fmt.Errorf("mq: persist offsets: %w", err)
	}
	return nil
}

func (s *offsetStore) all() map[string]map[string]map[int]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]map[int]int64, len(s.offsets))
	for g, topics := range s.offsets {
		out[g] = make(map[string]map[int]int64, len(topics))
		for t, parts := range topics {
			cp := make(map[int]int64, len(parts))
			for k, v := range parts {
				cp[k] = v
			}
			out[g][t] = cp
		}
	}
	return out
}
