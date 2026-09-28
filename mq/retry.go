package mq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RetryPolicy 是确定性的重试退避策略：
// backoff(attempt) = min(BaseDelay * Multiplier^(attempt-1), MaxDelay)，
// 不含随机抖动，同样的失败次数得到同样的等待，便于复现与测试。
type RetryPolicy struct {
	MaxAttempts int // 最大投递次数（含首次）；超过则进死信
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// DefaultRetryPolicy：最多 3 次投递，10ms 起步、上限 1s、倍率 2。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2}
}

// backoff 返回第 attempt 次投递失败后需要等待的时长（attempt 从 1 开始）。
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.BaseDelay)
	for i := 1; i < attempt; i++ {
		d *= p.Multiplier
	}
	if d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}
	return time.Duration(d)
}

// retryEntry 是一条等待退避到期后重新投递的记录。
type retryEntry struct {
	Group      string    `json:"group"`
	Topic      string    `json:"topic"`
	Partition  int       `json:"partition"`
	Offset     int64     `json:"offset"`
	Attempt    int       `json:"attempt"` // 已发生的投递次数
	NotBefore  time.Time `json:"not_before"`
	LastReason string    `json:"last_reason"`
	Reasons    []string  `json:"reasons"` // 历次失败原因（含本次）
}

func (e retryEntry) tp() TopicPartition { return TopicPartition{e.Topic, e.Partition} }

// DLQEntry 保留进入死信的消息快照与原始失败原因链。
type DLQEntry struct {
	Group     string    `json:"group"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Record    Record    `json:"record"`
	Attempts  int       `json:"attempts"`
	Reasons   []string  `json:"reasons"`
	DeadAt    time.Time `json:"dead_at"`
	// Replayed 标记该死信是否已被某次重投请求处理过（死信记录本身保留以便审计）。
	Replayed bool `json:"replayed"`
	// ReplayID 为处理该死信的重投请求 ID（未重投为空）。
	ReplayID string `json:"replay_id"`
	// Origin 在“重投消息再次进死信”时保留其上一跳的完整来源；原始消息为空。
	Origin *ReplayOrigin `json:"origin,omitempty"`
}

// retryState 是持久化到磁盘的整体快照。
type retryState struct {
	Pending map[string][]retryEntry `json:"pending"`
	Dead    map[string][]DLQEntry   `json:"dead"`
}

// retryStore 持久化每个消费组的待重试记录与死信记录（单文件原子写）。
type retryStore struct {
	mu      sync.Mutex
	path    string
	pending map[string][]retryEntry
	dead    map[string][]DLQEntry
}

func retryFilePath(dir string) string {
	return filepath.Join(dir, "retries.json")
}

func newRetryStore(dir string) (*retryStore, error) {
	path := retryFilePath(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &retryStore{
		path:    path,
		pending: map[string][]retryEntry{},
		dead:    map[string][]DLQEntry{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	if len(data) > 0 {
		var st retryState
		if err := json.Unmarshal(data, &st); err != nil {
			return nil, err
		}
		if st.Pending != nil {
			s.pending = st.Pending
		}
		if st.Dead != nil {
			s.dead = st.Dead
		}
	}
	return s, nil
}

// schedule 登记一次失败后的重试。e.Attempt 为本次失败的投递序号，
// now 为失败发生时刻；若 attempt 已达上限则连同消息快照转入死信，
// 返回 ErrRetryExhausted，调用方可据此与普通重试入队区分。
func (s *retryStore) schedule(e retryEntry, rec Record, policy RetryPolicy, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e.Attempt >= policy.MaxAttempts {
		// 进入死信：合并同一条消息此前的失败原因，保留完整原因链。
		reasons := []string{e.LastReason}
		if old, ok := s.findLocked(e.Group, e.tp(), e.Offset); ok {
			reasons = append(append([]string(nil), old.Reasons...), e.LastReason)
			s.pending[e.Group] = removeEntry(s.pending[e.Group], old)
		}
		dlq := DLQEntry{
			Group:     e.Group,
			Topic:     e.Topic,
			Partition: e.Partition,
			Offset:    e.Offset,
			Record:    rec,
			Attempts:  e.Attempt,
			Reasons:   reasons,
			DeadAt:    now,
		}
		// 若本次进死信的是“重投消息”，挂接上一跳的完整来源（失败次数、
		// 原始位移、历次原因、原始生产时间），并把上一跳原因链拼到本次链前，
		// 形成跨重投周期可追溯的完整失败史。
		if o := originFromHeaders(rec.Headers); o != nil {
			dlq.Origin = o
			dlq.Reasons = append(append([]string(nil), o.Reasons...), dlq.Reasons...)
		}
		s.dead[e.Group] = append(s.dead[e.Group], dlq)
		if err := s.persistLocked(); err != nil {
			return err
		}
		return ErrRetryExhausted
	}

	// 登记/覆盖待重试项（同一条消息的失败是串行的，不会有两个活动项）。
	wait := policy.Backoff(e.Attempt)
	e.NotBefore = now.Add(wait)
	list := s.pending[e.Group]
	if old, ok := s.findInListLocked(list, e.tp(), e.Offset); ok {
		// 累积历史失败原因，保证死信中可复盘每一次失败。
		e.Reasons = append(append([]string(nil), old.Reasons...), e.LastReason)
		list = upsertEntry(list, e)
	} else {
		e.Reasons = []string{e.LastReason}
		list = append(list, e)
	}
	s.pending[e.Group] = list
	return s.persistLocked()
}

// due 返回某组当前已到退避时间、可重新投递的待重试项（不移除）。
func (s *retryStore) due(group string, now time.Time) []retryEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []retryEntry
	for _, e := range s.pending[group] {
		if !now.Before(e.NotBefore) {
			out = append(out, e)
		}
	}
	return out
}

// remove 在消息最终成功（或被显式确认）后移除待重试项。
func (s *retryStore) remove(group string, tp TopicPartition, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.findLocked(group, tp, offset); ok {
		s.pending[group] = removeEntry(s.pending[group], e)
		_ = s.persistLocked()
	}
}

// retryState 查询某条记录是否处于待重试状态。
func (s *retryStore) retryState(group string, tp TopicPartition, offset int64) (retryEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findLocked(group, tp, offset)
}

func (s *retryStore) deadLetters(group string) []DLQEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]DLQEntry(nil), s.dead[group]...)
	return out
}

// findDead 按 (组×分区×位移) 查找单条死信（重投组装新记录时取原始快照）。
func (s *retryStore) findDead(group string, k deadKey) (DLQEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findDeadLocked(group, k)
}

// deadAt 判断某位移是否已处于死信终态。
func (s *retryStore) deadAt(group string, tp TopicPartition, offset int64) (DLQEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findDeadLocked(group, deadKey{tp: tp, offset: offset})
}

func (s *retryStore) findDeadLocked(group string, k deadKey) (DLQEntry, bool) {
	for _, d := range s.dead[group] {
		if d.Offset == k.offset && d.Record.topicPartition() == k.tp {
			return d, true
		}
	}
	return DLQEntry{}, false
}

func (s *retryStore) findLocked(group string, tp TopicPartition, offset int64) (retryEntry, bool) {
	return s.findInListLocked(s.pending[group], tp, offset)
}

func (s *retryStore) findInListLocked(list []retryEntry, tp TopicPartition, offset int64) (retryEntry, bool) {
	for _, e := range list {
		if e.Offset == offset && e.tp() == tp {
			return e, true
		}
	}
	return retryEntry{}, false
}

func upsertEntry(list []retryEntry, e retryEntry) []retryEntry {
	for i := range list {
		if list[i].Offset == e.Offset && list[i].tp() == e.tp() {
			list[i] = e
			return list
		}
	}
	return append(list, e)
}

func removeEntry(list []retryEntry, target retryEntry) []retryEntry {
	out := list[:0]
	for _, e := range list {
		if e.Offset == target.Offset && e.tp() == target.tp() {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (s *retryStore) persistLocked() error {
	st := retryState{Pending: s.pending, Dead: s.dead}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// deadKey 唯一标识某组死信列表中的一条死信（同组内 组×分区×位移 唯一）。
type deadKey struct {
	tp     TopicPartition
	offset int64
}

// selectDead 按筛选条件挑选某组尚未被重投、也不处于 skip 集合
// （已被其它待完成重投请求认领）中的死信，顺序按 主题、分区、位移 稳定排列。
// 注意：调用方必须持有 broker 主锁，以保证“挑选—认领”与并发重投互斥。
func (s *retryStore) selectDead(group string, f ReplayFilter, skip map[deadKey]bool) []DLQEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	partSet := map[int]bool{}
	for _, p := range f.Partitions {
		partSet[p] = true
	}
	offSet := map[int64]bool{}
	for _, o := range f.Offsets {
		offSet[o] = true
	}
	var out []DLQEntry
	for _, d := range s.dead[group] {
		if d.Replayed || skip[deadKey{tp: d.Record.topicPartition(), offset: d.Offset}] {
			continue
		}
		if f.Topic != "" && d.Topic != f.Topic {
			continue
		}
		if len(partSet) > 0 && !partSet[d.Partition] {
			continue
		}
		if len(offSet) > 0 && !offSet[d.Offset] {
			continue
		}
		if !f.DeadAtFrom.IsZero() && d.DeadAt.Before(f.DeadAtFrom) {
			continue
		}
		if !f.DeadAtTo.IsZero() && d.DeadAt.After(f.DeadAtTo) {
			continue
		}
		if f.FailureReason != "" && !containsReason(d.Reasons, f.FailureReason) {
			continue
		}
		out = append(out, d)
	}
	sortDead(out)
	return out
}

func containsReason(reasons []string, sub string) bool {
	for _, r := range reasons {
		if strings.Contains(r, sub) {
			return true
		}
	}
	return false
}

func sortDead(list []DLQEntry) {
	for i := 1; i < len(list); i++ {
		for j := i; j > 0 && lessDead(list[j-1], list[j]); j-- {
			list[j-1], list[j] = list[j], list[j-1]
		}
	}
}

func lessDead(a, b DLQEntry) bool {
	if a.Topic != b.Topic {
		return a.Topic > b.Topic
	}
	if a.Partition != b.Partition {
		return a.Partition > b.Partition
	}
	return a.Offset > b.Offset
}

// markReplayed 把指定死信标记为“已由 requestID 重投”。死信记录本身保留，
// 以保证 DeadLetters 审计与原因链可追溯。
func (s *retryStore) markReplayed(group, requestID string, keys []deadKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	want := map[deadKey]bool{}
	for _, k := range keys {
		want[k] = true
	}
	list := s.dead[group]
	for i := range list {
		k := deadKey{tp: list[i].Record.topicPartition(), offset: list[i].Offset}
		if want[k] {
			list[i].Replayed = true
			list[i].ReplayID = requestID
		}
	}
	return s.persistLocked()
}
