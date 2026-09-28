package mq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
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

	// 重投认领标记（死信重投功能）：
	// ReplayedOpID 非空表示该死信已被某个重投操作认领，防止被重复入队；
	// ReplayedNewOffset 为 -1 表示已认领但新消息尚未确认入队，>=0 为入队后的新位移。
	ReplayedOpID      string    `json:"replayed_op_id,omitempty"`
	ReplayedAt        time.Time `json:"replayed_at,omitempty"`
	ReplayedNewOffset int64     `json:"replayed_new_offset,omitempty"`
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

// groupNames 返回所有出现过死信/待重试的消费组名。
func (s *retryStore) groupNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]struct{}{}
	for g := range s.dead {
		seen[g] = struct{}{}
	}
	for g := range s.pending {
		seen[g] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for g := range seen {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// findDeadLocked 在某组死信列表中定位 (tp, offset)。
func (s *retryStore) findDeadIndexLocked(group string, tp TopicPartition, offset int64) int {
	for i := range s.dead[group] {
		e := s.dead[group][i]
		if e.Offset == offset && e.Topic == tp.Topic && e.Partition == tp.Partition {
			return i
		}
	}
	return -1
}

// claimDead 把一条死信标记为“被重投操作 opID 认领（尚未完成入队）”，
// 并返回该死信的快照副本。
// 若死信不存在、或已被另一个重投操作认领，则返回 ok=false（防御性：
// 正常流程下 Broker 串行调用、候选已排除已认领项，不会走到这里）。
func (s *retryStore) claimDead(group, opID string, tp TopicPartition, offset int64, now time.Time) (DLQEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.findDeadIndexLocked(group, tp, offset)
	if i < 0 {
		return DLQEntry{}, false
	}
	if existing := s.dead[group][i].ReplayedOpID; existing != "" && existing != opID {
		return DLQEntry{}, false
	}
	s.dead[group][i].ReplayedOpID = opID
	s.dead[group][i].ReplayedAt = now
	s.dead[group][i].ReplayedNewOffset = -1
	out := s.dead[group][i]
	_ = s.persistLocked()
	return out, true
}

// markReplayedEnqueued 记录重投消息已确认入队及其新位移。
func (s *retryStore) markReplayedEnqueued(group, opID string, tp TopicPartition, offset, newOffset int64, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.findDeadIndexLocked(group, tp, offset)
	if i < 0 {
		return
	}
	s.dead[group][i].ReplayedOpID = opID
	s.dead[group][i].ReplayedAt = now
	s.dead[group][i].ReplayedNewOffset = newOffset
	_ = s.persistLocked()
}

// claimedDead 返回某组“已认领但尚未确认入队（新位移为 -1）”的死信，
// 供重投账本在崩溃恢复时补全操作记录。
func (s *retryStore) claimedDead(group string) []DLQEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DLQEntry
	for _, e := range s.dead[group] {
		if e.ReplayedOpID != "" && e.ReplayedNewOffset < 0 {
			out = append(out, e)
		}
	}
	return out
}

// deadEntry 读取某组指定 (tp, offset) 的单条死信快照。
func (s *retryStore) deadEntry(group string, tp TopicPartition, offset int64) (DLQEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.findDeadIndexLocked(group, tp, offset)
	if i < 0 {
		return DLQEntry{}, false
	}
	return s.dead[group][i], true
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
