package mq

import (
	"fmt"
	"sort"
	"time"
)

// retryKey identifies a message position within one consumer group.
// Retry state is per-group so two groups consuming the same partition
// never interfere with each other.
type retryKey struct {
	group     string
	topic     string
	partition int
	offset    int64
}

func (k retryKey) String() string {
	return fmt.Sprintf("%s|%s|%d|%d", k.group, k.topic, k.partition, k.offset)
}

// pendingRetry is a nacked message waiting for its backoff to elapse.
type pendingRetry struct {
	Group     string    `json:"group"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Attempts  int       `json:"attempts"`
	Reason    string    `json:"reason"`
	NotBefore time.Time `json:"not_before"`
}

func (r *pendingRetry) key() retryKey {
	return retryKey{group: r.Group, topic: r.Topic, partition: r.Partition, offset: r.Offset}
}

// retryStore keeps pending retries (persisted across restarts).
type retryStore struct {
	path    string
	pending map[retryKey]*pendingRetry
}

func openRetryStore(path string) (*retryStore, error) {
	s := &retryStore{path: path, pending: make(map[retryKey]*pendingRetry)}
	var items []*pendingRetry
	if err := readJSON(path, &items); err != nil {
		return nil, fmt.Errorf("mq: load retries: %w", err)
	}
	for _, r := range items {
		s.pending[r.key()] = r
	}
	return s, nil
}

func (s *retryStore) add(r *pendingRetry) error {
	s.pending[r.key()] = r
	return s.persistLocked()
}

func (s *retryStore) remove(k retryKey) error {
	if _, ok := s.pending[k]; !ok {
		return nil
	}
	delete(s.pending, k)
	return s.persistLocked()
}

func (s *retryStore) get(k retryKey) *pendingRetry { return s.pending[k] }

func (s *retryStore) forGroupPartition(group, topic string, partition int) []*pendingRetry {
	var out []*pendingRetry
	for _, r := range s.pending {
		if r.Group == group && r.Topic == topic && r.Partition == partition {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
	return out
}

func (s *retryStore) countForPartition(topic string, partition int) int {
	n := 0
	for _, r := range s.pending {
		if r.Topic == topic && r.Partition == partition {
			n++
		}
	}
	return n
}

func (s *retryStore) count() int { return len(s.pending) }

// nextWakeup returns the earliest NotBefore among pending retries.
func (s *retryStore) nextWakeup(now time.Time) (time.Time, bool) {
	var next time.Time
	found := false
	for _, r := range s.pending {
		if r.NotBefore.After(now) && (!found || r.NotBefore.Before(next)) {
			next = r.NotBefore
			found = true
		}
	}
	return next, found
}

func (s *retryStore) persistLocked() error {
	items := make([]*pendingRetry, 0, len(s.pending))
	for _, r := range s.pending {
		items = append(items, r)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Group != items[j].Group {
			return items[i].Group < items[j].Group
		}
		if items[i].Topic != items[j].Topic {
			return items[i].Topic < items[j].Topic
		}
		if items[i].Partition != items[j].Partition {
			return items[i].Partition < items[j].Partition
		}
		return items[i].Offset < items[j].Offset
	})
	if err := writeJSONAtomically(s.path, items); err != nil {
		return fmt.Errorf("mq: persist retries: %w", err)
	}
	return nil
}
