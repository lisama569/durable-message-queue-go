package mq

import (
	"fmt"
	"time"
)

// DeadLetter is a message that exhausted its retry budget.
type DeadLetter struct {
	Group        string            `json:"group"`
	Topic        string            `json:"topic"`
	Partition    int               `json:"partition"`
	Offset       int64             `json:"offset"`
	Key          string            `json:"key"`
	Headers      map[string]string `json:"headers"`
	Body         []byte            `json:"body"`
	Attempts     int               `json:"attempts"`
	Reason       string            `json:"reason"`
	ProducedAt   time.Time         `json:"produced_at"`
	DeadLettered time.Time         `json:"dead_lettered_at"`
}

type dlqFile struct {
	Items []DeadLetter `json:"items"`
}

// dlqStore is the persisted dead-letter queue (append-only, per broker).
type dlqStore struct {
	path  string
	items []DeadLetter
	index map[retryKey]int
}

func openDLQStore(path string) (*dlqStore, error) {
	s := &dlqStore{path: path, index: make(map[retryKey]int)}
	var f dlqFile
	if err := readJSON(path, &f); err != nil {
		return nil, fmt.Errorf("mq: load dlq: %w", err)
	}
	s.items = f.Items
	if s.items == nil {
		s.items = []DeadLetter{}
	}
	for i, d := range s.items {
		s.index[retryKey{d.Group, d.Topic, d.Partition, d.Offset}] = i
	}
	return s, nil
}

// put appends d. A dead letter for the same group+coordinates is ignored so
// a crash between DLQ write and offset advance cannot duplicate it.
func (s *dlqStore) put(d DeadLetter) error {
	k := retryKey{d.Group, d.Topic, d.Partition, d.Offset}
	if _, exists := s.index[k]; exists {
		return nil
	}
	s.index[k] = len(s.items)
	s.items = append(s.items, d)
	if err := s.persistLocked(); err != nil {
		delete(s.index, k)
		s.items = s.items[:len(s.items)-1]
		return fmt.Errorf("mq: persist dlq: %w", err)
	}
	return nil
}

func (s *dlqStore) list() []DeadLetter {
	out := make([]DeadLetter, len(s.items))
	copy(out, s.items)
	return out
}

func (s *dlqStore) count() int { return len(s.items) }

func (s *dlqStore) persistLocked() error {
	if err := writeJSONAtomically(s.path, &dlqFile{Items: s.items}); err != nil {
		return err
	}
	return nil
}
