package mq

import (
	"context"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const stateDir = "_state"
const logDir = "logs"

// waitTick is the maximal cond.Wait interval; waits are also woken eagerly
// by Broadcast on produce/nack/rebalance.
const waitTick = 5 * time.Millisecond

// Broker is an in-process, disk-backed message queue with consumer groups.
//
// All coordinator/store methods are invoked while holding b.mu, so the
// coordinator intentionally has no lock of its own.
type Broker struct {
	cfg      BrokerConfig
	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
	doneCh   chan struct{} // closed once on Close
	root     string
	rr       map[string]int64
	topics   map[string][]*partition
	offsets  *offsetStore
	retries  *retryStore
	dlq      *dlqStore
	coord    *coordinator
	inflight map[string]map[retryKey]int64 // group -> key -> delivery count
}

type topicsFile struct {
	Topics []topicDesc `json:"topics"`
}

type topicDesc struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

// Open opens or recovers a broker rooted at cfg.RootDir.
func Open(cfg BrokerConfig) (*Broker, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.Backoff == nil {
		// Deterministic default: no randomness, so redelivery schedules are
		// reproducible across runs and restarts.
		cfg.Backoff = ConstantBackoff{D: 200 * time.Millisecond}
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = 5 * time.Second
	}
	root := cfg.RootDir
	b := &Broker{
		cfg:      cfg,
		root:     root,
		doneCh:   make(chan struct{}),
		rr:       make(map[string]int64),
		topics:   make(map[string][]*partition),
		inflight: make(map[string]map[retryKey]int64),
	}
	b.cond = sync.NewCond(&b.mu)
	var err error
	statePath := filepath.Join(root, stateDir)
	if b.offsets, err = openOffsetStore(filepath.Join(statePath, "offsets.json")); err != nil {
		return nil, err
	}
	if b.retries, err = openRetryStore(filepath.Join(statePath, "retries.json")); err != nil {
		return nil, err
	}
	if b.dlq, err = openDLQStore(filepath.Join(statePath, "dlq.json")); err != nil {
		return nil, err
	}
	if b.coord, err = openCoordinator(filepath.Join(statePath, "groups.json"), cfg.LeaseTTL, cfg.Now); err != nil {
		return nil, err
	}
	// Partitions revoked on rebalance must have their in-flight delivery
	// invalidated, otherwise a redelivery could be silently skipped.
	b.coord.setRevokeHook(b.onRevokeLocked)

	// Recover topic layout and partition logs. Restoring every topic before
	// publishing the universe once avoids spurious rebalances (which would
	// bump generations) during recovery.
	var tf topicsFile
	if err := readJSON(filepath.Join(statePath, "topics.json"), &tf); err != nil {
		return nil, err
	}
	for _, d := range tf.Topics {
		if err := b.restoreTopicLocked(d.Name, d.Partitions); err != nil {
			return nil, err
		}
	}
	if err := b.coord.setUniverse(b.allTPsLocked()); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) onRevokeLocked(groupID string, revoked []TopicPartition, gen int64) {
	inf, ok := b.inflight[groupID]
	if !ok {
		return
	}
	for _, tp := range revoked {
		for off := range inf {
			if off.topic == tp.Topic && off.partition == tp.Partition {
				delete(inf, off)
			}
		}
	}
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	close(b.doneCh)
	var firstErr error
	for _, parts := range b.topics {
		for _, p := range parts {
			if err := p.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	b.cond.Broadcast()
	return firstErr
}

func (b *Broker) checkClosedLocked() error {
	if b.closed {
		return ErrClosed
	}
	return nil
}

// CreateTopic creates a topic with the broker's configured partition count.
// Creating an existing topic is an idempotent no-op.
func (b *Broker) CreateTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkClosedLocked(); err != nil {
		return err
	}
	if _, ok := b.topics[topic]; ok {
		return nil
	}
	n := b.cfg.Partitions
	if err := b.restoreTopicLocked(topic, n); err != nil {
		return err
	}
	if err := b.coord.setUniverse(b.allTPsLocked()); err != nil {
		return err
	}
	return b.persistTopicsLocked()
}

// restoreTopicLocked opens (or recovers) a topic's partition logs without
// touching group universes or generations.
func (b *Broker) restoreTopicLocked(topic string, n int) error {
	if _, ok := b.topics[topic]; ok {
		return nil
	}
	parts := make([]*partition, n)
	for i := 0; i < n; i++ {
		p, err := newPartition(filepath.Join(b.root, logDir), topic, i, b.cfg.PartitionCap, b.cfg.HighWatermark)
		if err != nil {
			return err
		}
		parts[i] = p
	}
	b.topics[topic] = parts
	return nil
}

func (b *Broker) persistTopicsLocked() error {
	var tf topicsFile
	for name, parts := range b.topics {
		tf.Topics = append(tf.Topics, topicDesc{Name: name, Partitions: len(parts)})
	}
	sort.Slice(tf.Topics, func(i, j int) bool { return tf.Topics[i].Name < tf.Topics[j].Name })
	return writeJSONAtomically(filepath.Join(b.root, stateDir, "topics.json"), &tf)
}

func (b *Broker) allTPsLocked() []TopicPartition {
	var out []TopicPartition
	for name, parts := range b.topics {
		for i := range parts {
			out = append(out, TopicPartition{Topic: name, Partition: i})
		}
	}
	sortTPs(out)
	return out
}

func (b *Broker) partitionLocked(tp TopicPartition) (*partition, error) {
	parts, ok := b.topics[tp.Topic]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownTopic, tp.Topic)
	}
	if tp.Partition < 0 || tp.Partition >= len(parts) {
		return nil, fmt.Errorf("%w: %s[%d]", ErrUnknownPartition, tp.Topic, tp.Partition)
	}
	return parts[tp.Partition], nil
}

// minCommittedLocked returns the smallest committed offset for a partition
// across all groups. A group with no commit is implicitly at 0 (it has the
// whole log to consume), so with no commits this returns 0. This slowest
// consumer defines backlog depth and therefore capacity.
func (b *Broker) minCommittedLocked(topic string, partition int) int64 {
	min := int64(0)
	found := false
	for _, topics := range b.offsets.all() {
		if parts, ok := topics[topic]; ok {
			if c, ok := parts[partition]; ok {
				if !found || c < min {
					min = c
				}
				found = true
			}
		}
	}
	_ = found
	return min
}

// lagLocked computes the slowest-consumer backlog of one partition.
func (b *Broker) lagLocked(p *partition) int64 {
	lag := p.logEnd() - b.minCommittedLocked(p.topic, p.id)
	if lag < 0 {
		return 0
	}
	return lag
}

// Produce appends a message. A non-empty key hashes to a stable partition
// (ordering per key); otherwise partitions are chosen round-robin.
// Under backpressure it blocks until space frees or ctx expires; messages
// are never silently dropped or duplicated.
func (b *Broker) Produce(ctx context.Context, topic string, msg Message) (TopicPartition, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	parts, ok := b.topics[topic]
	if !ok {
		return TopicPartition{}, 0, fmt.Errorf("%w: %s", ErrUnknownTopic, topic)
	}
	if err := b.checkClosedLocked(); err != nil {
		return TopicPartition{}, 0, err
	}
	n := len(parts)
	var id int
	if msg.Key != "" {
		h := fnv.New32a()
		h.Write([]byte(msg.Key))
		id = int(h.Sum32() % uint32(n))
	} else {
		id = int(b.rr[topic] % int64(n))
	}
	p := parts[id]
	// Wait for capacity (backpressure) measured by slowest-consumer lag.
	// The waiting message is never dropped or duplicated.
	for {
		if !p.full(b.lagLocked(p)) {
			break
		}
		if err := b.waitLocked(ctx); err != nil {
			return TopicPartition{}, 0, err
		}
	}
	off, err := p.append(msg, b.cfg.Now())
	if err != nil {
		return TopicPartition{}, 0, err
	}
	if msg.Key == "" {
		b.rr[topic] = (b.rr[topic] + 1) % int64(n)
	}
	tp := TopicPartition{Topic: topic, Partition: id}
	b.cond.Broadcast()
	return tp, off, nil
}

// waitLocked atomically releases b.mu, waits up to waitTick for a signal
// (or ctx cancellation), then re-acquires b.mu. cond.Wait must run on the
// calling goroutine while it logically holds the lock; a helper goroutine
// is only allowed to call Broadcast.
func (b *Broker) waitLocked(ctx context.Context) error {
	return b.waitForSignalLocked(ctx, waitTick)
}

func (b *Broker) waitForSignalLocked(ctx context.Context, maxWait time.Duration) error {
	d := maxWait
	if d <= 0 || d > waitTick {
		d = waitTick
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	go func() {
		select {
		case <-b.doneCh:
			b.cond.Broadcast()
		case <-ctx.Done():
			b.cond.Broadcast()
		case <-timer.C:
			b.cond.Broadcast()
		}
	}()
	b.cond.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.checkClosedLocked()
}

// JoinGroup registers a consumer and returns its generation-scoped assignments.
func (b *Broker) JoinGroup(groupID, memberID string) (int64, []TopicPartition, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkClosedLocked(); err != nil {
		return 0, nil, err
	}
	gen, tps, err := b.coord.join(groupID, memberID, b.allTPsLocked())
	if err != nil {
		return 0, nil, err
	}
	b.cond.Broadcast()
	return gen, tps, nil
}

func (b *Broker) Heartbeat(groupID, memberID string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkClosedLocked(); err != nil {
		return 0, err
	}
	gen, err := b.coord.heartbeat(groupID, memberID)
	if err != nil {
		return 0, err
	}
	b.cond.Broadcast()
	return gen, nil
}

func (b *Broker) LeaveGroup(groupID, memberID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkClosedLocked(); err != nil {
		return err
	}
	if err := b.coord.leave(groupID, memberID); err != nil {
		return err
	}
	b.cond.Broadcast()
	return nil
}

func (b *Broker) groupInflightLocked(group string) map[retryKey]int64 {
	m, ok := b.inflight[group]
	if !ok {
		m = make(map[retryKey]int64)
		b.inflight[group] = m
	}
	return m
}

// Fetch blocks until the next message for tp is available to the member.
func (b *Broker) Fetch(ctx context.Context, groupID, memberID string, gen int64, tp TopicPartition) (Delivered, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, err := b.partitionLocked(tp)
	if err != nil {
		return Delivered{}, err
	}
	if err := b.checkClosedLocked(); err != nil {
		return Delivered{}, err
	}
	inflight := b.groupInflightLocked(groupID)
	for {
		if err := b.coord.checkOwner(groupID, memberID, tp, gen); err != nil {
			return Delivered{}, err
		}
		off := b.offsets.get(groupID, tp.Topic, tp.Partition)
		// Head-of-line blocking: honour the deterministic retry backoff
		// rather than jumping over the message (no silent skips).
		k := retryKey{groupID, tp.Topic, tp.Partition, off}
		if pr := b.retries.get(k); pr != nil && b.cfg.Now().Before(pr.NotBefore) {
			if err := b.waitUntilLocked(ctx, pr.NotBefore); err != nil {
				return Delivered{}, err
			}
			continue
		}
		end := p.logEnd()
		if off >= end {
			// No new messages yet: block until produce/nack/redelivery.
			if err := b.waitLocked(ctx); err != nil {
				return Delivered{}, err
			}
			continue
		}
		if _, busy := inflight[k]; busy {
			// Partition exclusivity makes this unreachable normally; wait
			// defensively instead of redelivering concurrently.
			if err := b.waitLocked(ctx); err != nil {
				return Delivered{}, err
			}
			continue
		}
		rec, err := p.read(off)
		if err != nil {
			return Delivered{}, err
		}
		delivery := int64(1)
		if pr := b.retries.get(k); pr != nil {
			delivery = int64(pr.Attempts) + 1
		}
		inflight[k] = delivery
		msg := Message{
			Key:     rec.Key,
			Headers: rec.Headers,
			Body:    rec.Body,
			Offset:  rec.Offset,
			Time:    time.Unix(0, rec.TimeNS),
		}
		return Delivered{Topic: tp.Topic, Partition: tp.Partition, Message: msg, Delivery: delivery}, nil
	}
}

// waitUntilLocked waits until wall-clock now reaches t, ctx expires, or
// the broker wakes first (rebalance/shutdown).
func (b *Broker) waitUntilLocked(ctx context.Context, t time.Time) error {
	for b.cfg.Now().Before(t) {
		if err := b.waitLocked(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Ack acknowledges d and commits offset+1 (idempotent).
func (b *Broker) Ack(groupID, memberID string, gen int64, d Delivered) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	tp := TopicPartition{d.Topic, d.Partition}
	if _, err := b.partitionLocked(tp); err != nil {
		return err
	}
	if err := b.coord.checkOwner(groupID, memberID, tp, gen); err != nil {
		return err
	}
	k := retryKey{groupID, d.Topic, d.Partition, d.Message.Offset}
	inflight := b.groupInflightLocked(groupID)
	delete(inflight, k)
	if err := b.retries.remove(k); err != nil {
		return err
	}
	p, err := b.partitionLocked(tp)
	if err != nil {
		return err
	}
	if err := b.offsets.commit(groupID, d.Topic, d.Partition, d.Message.Offset+1, p.logEnd()); err != nil {
		return err
	}
	// Consuming a message can free hard capacity (depth shrinks); wake any
	// producers blocked under backpressure.
	b.cond.Broadcast()
	return nil
}

// Nack records a processing failure with deterministic retry/dead-letter rules.
func (b *Broker) Nack(groupID, memberID string, gen int64, d Delivered, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	tp := TopicPartition{d.Topic, d.Partition}
	if _, err := b.partitionLocked(tp); err != nil {
		return err
	}
	if err := b.coord.checkOwner(groupID, memberID, tp, gen); err != nil {
		return err
	}
	k := retryKey{groupID, d.Topic, d.Partition, d.Message.Offset}
	inflight := b.groupInflightLocked(groupID)
	delete(inflight, k)

	existing := b.retries.get(k)
	attempts := 1
	if existing != nil {
		attempts = existing.Attempts + 1
	}

	if attempts <= b.cfg.MaxRetries && b.cfg.Backoff != nil {
		delay := b.cfg.Backoff.Delay(attempts)
		pr := &pendingRetry{
			Group:     groupID,
			Topic:     d.Topic,
			Partition: d.Partition,
			Offset:    d.Message.Offset,
			Attempts:  attempts,
			Reason:    reason,
			NotBefore: b.cfg.Now().Add(delay),
		}
		if err := b.retries.add(pr); err != nil {
			return err
		}
	} else {
		// Budget exhausted: dead-letter (deduped), then advance past it.
		dl := DeadLetter{
			Group:        groupID,
			Topic:        d.Topic,
			Partition:    d.Partition,
			Offset:       d.Message.Offset,
			Key:          d.Message.Key,
			Headers:      d.Message.Headers,
			Body:         d.Message.Body,
			Attempts:     attempts,
			Reason:       reason,
			ProducedAt:   d.Message.Time,
			DeadLettered: b.cfg.Now(),
		}
		if err := b.dlq.put(dl); err != nil {
			return err
		}
		if err := b.retries.remove(k); err != nil {
			return err
		}
		if err := b.offsets.forceAdvance(groupID, d.Topic, d.Partition, d.Message.Offset+1); err != nil {
			return err
		}
	}
	b.cond.Broadcast()
	return nil
}

// CommitOffset explicitly commits an offset. Idempotent; regressions and
// out-of-range values are rejected with *OffsetError.
func (b *Broker) CommitOffset(groupID, memberID string, gen int64, tp TopicPartition, offset int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, err := b.partitionLocked(tp)
	if err != nil {
		return err
	}
	if err := b.coord.checkOwner(groupID, memberID, tp, gen); err != nil {
		return err
	}
	return b.offsets.commit(groupID, tp.Topic, tp.Partition, offset, p.logEnd())
}

func (b *Broker) PartitionStats(topic string, partition int) (PartitionStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tp := TopicPartition{Topic: topic, Partition: partition}
	p, err := b.partitionLocked(tp)
	if err != nil {
		return PartitionStats{}, err
	}
	var retryPending = b.retries.countForPartition(topic, partition)
	dlCount := 0
	for _, d := range b.dlq.list() {
		if d.Topic == topic && d.Partition == partition {
			dlCount++
		}
	}
	var inf int
	for _, m := range b.inflight {
		for k := range m {
			if k.topic == topic && k.partition == partition {
				inf++
			}
		}
	}
	committed := map[string]int64{}
	for g, topics := range b.offsets.all() {
		if parts, ok := topics[topic]; ok {
			if c, ok := parts[partition]; ok {
				committed[g] = c
			}
		}
	}
	lag := b.lagLocked(p)
	return PartitionStats{
		Topic:           topic,
		Partition:       partition,
		LogStart:        p.logStart(),
		LogEnd:          p.logEnd(),
		Lag:             lag,
		Committed:       committed,
		InFlight:        inf,
		RetryPending:    retryPending,
		DeadLettered:    dlCount,
		Watermark:       b.cfg.HighWatermark,
		AtHighWatermark: p.atHighWatermark(lag),
		Full:            p.full(lag),
	}, nil
}

func (b *Broker) GroupStats(groupID string) (GroupStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.coord.stats()[groupID]
	if !ok {
		return GroupStats{}, fmt.Errorf("%w: %s", ErrUnknownGroup, groupID)
	}
	return st, nil
}

func (b *Broker) DeadLetters() []DeadLetter {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dlq.list()
}
