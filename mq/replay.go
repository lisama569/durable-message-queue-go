package mq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 死信重投（dead-letter replay）：
//
// 重投不是“复活原位移”，而是把死信消息作为【一条新消息追加到同分区日志尾部】。
// 这样已提交位移无需回退（原死信位移保持终态），重投消息天然排在同分区所有
// 已存在消息之后，顺序、位移、租约语义全部沿用现有规则，并重新获得完整的
// 重试机会；原始来源、历次失败原因与失败次数通过消息头中的溯源信息保留。

// 重投溯源信息使用的保留消息头前缀；业务自定义消息头不要使用该前缀。
const (
	replayHeaderID        = "x-mq-replay-id"
	replayHeaderGroup     = "x-mq-replay-group"
	replayHeaderOriginTP  = "x-mq-replay-origin-topic-partition"
	replayHeaderOriginOff = "x-mq-replay-origin-offset"
	replayHeaderAttempts  = "x-mq-replay-origin-attempts"
	replayHeaderReasons   = "x-mq-replay-origin-reasons"
	replayHeaderEnqueued  = "x-mq-replay-origin-enqueued-at"
	replayHeaderDeadAt    = "x-mq-replay-dead-at"
	replayHeaderRound     = "x-mq-replay-round"
)

// ReplayFilter 描述从某个消费组的死信中挑选重投对象的条件。
// 零值字段不过滤；所有给出的字段之间为“与”关系。
type ReplayFilter struct {
	Topic           string    // 仅重投该主题（空表示不限主题）
	Partitions      []int     // 仅重投这些分区（nil/空表示不限分区）
	Offsets         []int64   // 精确指定原始位移（nil 表示不按位移筛选）
	ReasonContains  string    // 原始失败原因链中包含该子串（空表示不按原因筛选）
	DeadFrom        time.Time // 死信产生时间下界（含）；零值表示不限
	DeadTo          time.Time // 死信产生时间上界（含）；零值表示不限
	IncludeReplayed bool      // 是否允许挑选此前已经重投过的死信（默认不允许）
}

// ReplayStatus 为重投操作的状态。
type ReplayStatus string

const (
	ReplayPending  ReplayStatus = "pending"  // 尚有未完成的条目
	ReplayComplete ReplayStatus = "complete" // 全部条目已重新入队
	ReplayFailed   ReplayStatus = "failed"   // 暂不使用的预留状态
)

// ReplayItem 是重投操作中针对单条死信的处理状态。
type ReplayItem struct {
	Topic        string       `json:"topic"`
	Partition    int          `json:"partition"`
	OriginOffset int64        `json:"origin_offset"`
	Status       ReplayStatus `json:"status"`
	NewOffset    int64        `json:"new_offset"` // 重新入队后分配到的新位移（-1 表示尚未入队）
	ScanFrom     int64        `json:"scan_from"`  // 恢复时用于幂等探测的日志扫描起点
	ReservedAt   time.Time    `json:"reserved_at,omitempty"`
	EnqueuedAt   time.Time    `json:"enqueued_at,omitempty"`
	LastError    string       `json:"last_error,omitempty"`
}

func (i ReplayItem) tp() TopicPartition { return TopicPartition{i.Topic, i.Partition} }

// ReplayOperation 是一次（幂等的）死信重投操作的完整可查询记录。
type ReplayOperation struct {
	OpID           string       `json:"op_id"`
	Group          string       `json:"group"`
	IdempotencyKey string       `json:"idempotency_key"`
	Status         ReplayStatus `json:"status"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
	Items          []ReplayItem `json:"items"`
}

// ReplayResult 是触发重投的返回结果：携带操作记录与本次实际新入队的条目数。
type ReplayResult struct {
	Operation   ReplayOperation
	Matched     int // 命中筛选条件的死信条数
	EnqueuedNow int // 本次调用实际完成入队的条目数
	AlreadyDone int // 命中但此前已经重投过（未重复入队）的条目数
}

// ReplayProvenance 是从重投消息头中解析出的原始溯源信息。
type ReplayProvenance struct {
	OpID            string
	Group           string
	OriginTopic     string
	OriginPartition int
	OriginOffset    int64
	OriginAttempts  int
	OriginReasons   []string
	OriginEnqueued  time.Time
	DeadAt          time.Time
	Round           int
}

// replayState 是重投操作账本的磁盘快照。
type replayState struct {
	Ops []ReplayOperation `json:"ops"`
}

// replayStore 持久化重投操作账本（replays.json，临时文件 + 原子 rename）。
//
// 所有变更都由 Broker 的互斥锁串行化；本结构额外持有自己的 mu，
// 使 GetReplay/ListReplays 等只读查询可以与“触发/续跑”并发执行而不发生
// 数据竞争。所有对外返回的操作均为深拷贝快照：查询到的要么是推进前的
// 完整状态、要么是推进后的完整状态，绝不会看到条目更新一半的中间态。
type replayStore struct {
	mu   sync.Mutex
	path string
	ops  map[string]*ReplayOperation
}

func replayFilePath(dir string) string {
	return filepath.Join(dir, "replays.json")
}

func newReplayStore(dir string) (*replayStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &replayStore{path: replayFilePath(dir), ops: map[string]*ReplayOperation{}}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	if len(data) > 0 {
		var st replayState
		if err := json.Unmarshal(data, &st); err != nil {
			return nil, err
		}
		for i := range st.Ops {
			op := st.Ops[i]
			s.ops[op.OpID] = &op
		}
	}
	return s, nil
}

func (s *replayStore) clone(op *ReplayOperation) ReplayOperation {
	out := *op
	out.Items = append([]ReplayItem(nil), op.Items...)
	return out
}

func (s *replayStore) getOp(opID string) (ReplayOperation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[opID]
	if !ok {
		return ReplayOperation{}, false
	}
	return s.clone(op), true
}

func (s *replayStore) findByIdempotency(group, key string) (ReplayOperation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range s.ops {
		if op.Group == group && op.IdempotencyKey == key {
			return s.clone(op), true
		}
	}
	return ReplayOperation{}, false
}

func (s *replayStore) listOps(group string) []ReplayOperation {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ReplayOperation
	for _, op := range s.ops {
		if op.Group == group {
			out = append(out, s.clone(op))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].OpID < out[j].OpID
	})
	return out
}

// newOpID 生成账本内唯一的操作 ID（Broker 串行调用，计数天然唯一；
// 再加上时间戳与随机后缀以避免跨进程/目录复用碰撞）。
func newOpID(now time.Time, seq int64) string {
	return "replay-" + strconv.FormatInt(now.UnixNano(), 36) + "-" + strconv.FormatInt(seq, 36)
}

// createReplay 在账本中登记一次重投操作并逐条认领入选死信。
//
// 顺序刻意设计为【账本先落盘、再认领死信】：这样任何崩溃窗口里，只要
// 认领标记可能已落盘，账本操作（连同调用方幂等键）就一定已经持久化。
// 重启后调用方用同一幂等键重试必然恢复到同一个操作，由 runReplayLocked
// 的日志尾部探测保证不重复入队，不存在“认领了却找不到操作”的孤儿窗口。
//
// 返回实际认领成功的死信快照；认领失败（不存在/已被他人认领）的条目会
// 从操作中移除，绝不凭快照凭空入队。
func (s *replayStore) createReplay(rs *retryStore, group, idemKey string,
	candidates []DLQEntry, now time.Time, seq int64) (*ReplayOperation, []DLQEntry) {

	s.mu.Lock()
	defer s.mu.Unlock()
	op := &ReplayOperation{
		OpID:           newOpID(now, seq),
		Group:          group,
		IdempotencyKey: idemKey,
		Status:         ReplayPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	// 阶段一：候选条目先全部入账本并落盘（此时尚未触碰死信认领标记）。
	for _, e := range candidates {
		op.Items = append(op.Items, ReplayItem{
			Topic:        e.Topic,
			Partition:    e.Partition,
			OriginOffset: e.Offset,
			Status:       ReplayPending,
			NewOffset:    -1,
			ScanFrom:     e.Record.Offset + 1,
			ReservedAt:   now,
		})
	}
	s.ops[op.OpID] = op
	_ = s.persistLocked()

	// 阶段二：逐条认领死信；认领失败者从账本剔除（不阻塞其余条目）。
	var claimed []DLQEntry
	kept := op.Items[:0]
	for i, e := range candidates {
		tp := TopicPartition{Topic: e.Topic, Partition: e.Partition}
		snap, ok := rs.claimDead(group, op.OpID, tp, e.Offset, now)
		if !ok {
			continue
		}
		item := op.Items[i]
		item.ScanFrom = snap.Record.Offset + 1
		kept = append(kept, item)
		claimed = append(claimed, snap)
	}
	op.Items = kept
	if len(op.Items) == 0 {
		op.Status = ReplayComplete
	}
	op.UpdatedAt = now
	_ = s.persistLocked()
	return op, claimed
}

// markReserved 把条目置为“已预留位移、即将写日志”。
func (s *replayStore) markReserved(opID string, item ReplayItem, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[opID]
	if !ok {
		return false
	}
	for i := range op.Items {
		if op.Items[i].Topic == item.Topic &&
			op.Items[i].Partition == item.Partition &&
			op.Items[i].OriginOffset == item.OriginOffset {
			op.Items[i].Status = ReplayPending
			op.Items[i].NewOffset = item.NewOffset
			op.Items[i].ScanFrom = item.NewOffset
			op.Items[i].ReservedAt = now
			op.Items[i].LastError = ""
			op.UpdatedAt = now
			_ = s.persistLocked()
			return true
		}
	}
	return false
}

// markEnqueued 把条目置为完成并记录新位移；全部完成时操作整体置 complete。
func (s *replayStore) markEnqueued(opID string, item ReplayItem, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[opID]
	if !ok {
		return false
	}
	for i := range op.Items {
		if op.Items[i].Topic == item.Topic &&
			op.Items[i].Partition == item.Partition &&
			op.Items[i].OriginOffset == item.OriginOffset {
			op.Items[i].Status = ReplayComplete
			op.Items[i].NewOffset = item.NewOffset
			op.Items[i].ScanFrom = item.NewOffset
			op.Items[i].EnqueuedAt = now
			op.Items[i].LastError = ""
		}
	}
	allDone := true
	for _, it := range op.Items {
		if it.Status != ReplayComplete {
			allDone = false
		}
	}
	if allDone {
		op.Status = ReplayComplete
	}
	op.UpdatedAt = now
	_ = s.persistLocked()
	return true
}

// markItemError 记录条目本次推进失败原因（不改变其 pending 状态，可稍后重试）。
func (s *replayStore) markItemError(opID string, item ReplayItem, msg string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[opID]
	if !ok {
		return
	}
	for i := range op.Items {
		if op.Items[i].Topic == item.Topic &&
			op.Items[i].Partition == item.Partition &&
			op.Items[i].OriginOffset == item.OriginOffset {
			op.Items[i].LastError = msg
			op.Items[i].NewOffset = -1
		}
	}
	op.UpdatedAt = now
	_ = s.persistLocked()
}

// reconcile 用死信表中“已认领但账本里找不到对应操作”的记录补全账本。
// 这对应“认领标记已落盘、但操作账本尚未落盘就崩溃”的窗口；补出的操作
// 在 Resume 时通过日志尾部探测保证不重复入队。
func (s *replayStore) reconcile(rs *retryStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for _, g := range rs.groupNames() {
		for _, e := range rs.claimedDead(g) {
			if s.opExistsForClaim(e.ReplayedOpID, e) {
				continue
			}
			now := e.ReplayedAt
			op := &ReplayOperation{
				OpID:           e.ReplayedOpID,
				Group:          g,
				IdempotencyKey: "recovered:" + e.ReplayedOpID,
				Status:         ReplayPending,
				CreatedAt:      now,
				UpdatedAt:      now,
				Items: []ReplayItem{{
					Topic:        e.Topic,
					Partition:    e.Partition,
					OriginOffset: e.Offset,
					Status:       ReplayPending,
					NewOffset:    -1,
					ScanFrom:     e.Record.Offset + 1,
					ReservedAt:   now,
				}},
			}
			if _, exists := s.ops[op.OpID]; !exists {
				s.ops[op.OpID] = op
				changed = true
			}
		}
	}
	if changed {
		_ = s.persistLocked()
	}
}

func (s *replayStore) opExistsForClaim(opID string, e DLQEntry) bool {
	op, ok := s.ops[opID]
	if !ok {
		return false
	}
	for _, it := range op.Items {
		if it.Topic == e.Topic && it.Partition == e.Partition && it.OriginOffset == e.Offset {
			return true
		}
	}
	return false
}

// persistLocked 原子落盘账本；调用方必须持有 s.mu。
func (s *replayStore) persistLocked() error {
	st := replayState{Ops: make([]ReplayOperation, 0, len(s.ops))}
	for _, op := range s.ops {
		st.Ops = append(st.Ops, s.clone(op))
	}
	sort.Slice(st.Ops, func(i, j int) bool {
		if !st.Ops[i].CreatedAt.Equal(st.Ops[j].CreatedAt) {
			return st.Ops[i].CreatedAt.Before(st.Ops[j].CreatedAt)
		}
		return st.Ops[i].OpID < st.Ops[j].OpID
	})
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

// inInts 判断 n 是否在列表中。
func inInts(list []int, n int) bool {
	for _, v := range list {
		if v == n {
			return true
		}
	}
	return false
}

func inInt64s(list []int64, n int64) bool {
	for _, v := range list {
		if v == n {
			return true
		}
	}
	return false
}

// selectReplayCandidates 按筛选条件从死信列表中挑出候选，
// 输出按 (topic, partition, originOffset) 稳定排序，保证同分区内重投入队顺序确定。
func selectReplayCandidates(dead []DLQEntry, f ReplayFilter) []DLQEntry {
	out := make([]DLQEntry, 0, len(dead))
	for _, e := range dead {
		if !f.IncludeReplayed && e.ReplayedOpID != "" {
			continue
		}
		if f.Topic != "" && e.Topic != f.Topic {
			continue
		}
		if len(f.Partitions) > 0 && !inInts(f.Partitions, e.Partition) {
			continue
		}
		if len(f.Offsets) > 0 && !inInt64s(f.Offsets, e.Offset) {
			continue
		}
		if f.ReasonContains != "" && !strings.Contains(strings.Join(e.Reasons, "\n"), f.ReasonContains) {
			continue
		}
		if !f.DeadFrom.IsZero() && e.DeadAt.Before(f.DeadFrom) {
			continue
		}
		if !f.DeadTo.IsZero() && e.DeadAt.After(f.DeadTo) {
			continue
		}
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		if out[i].Partition != out[j].Partition {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Offset < out[j].Offset
	})
	return out
}

// stripReplayHeaders 移除可能已有的保留重投头，避免与新写入的溯源信息冲突。
func stripReplayHeaders(h map[string]string) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		if strings.HasPrefix(k, "x-mq-replay-") {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// buildReplayMessage 由死信快照构造重投的新消息体：
// 业务 Key/Value/Headers 原样保留，并写入完整溯源（原始来源、失败次数、原因链）。
// 若死信本身来自更早一次重投，溯源仍指向“最初来源”且重投代次递增，
// 形成可追溯的重投链。
func buildReplayMessage(opID string, e DLQEntry, now time.Time) Message {
	h := stripReplayHeaders(e.Record.Headers)
	if h == nil {
		h = map[string]string{}
	}
	tp := TopicPartition{Topic: e.Topic, Partition: e.Partition}
	originTP := tp.String()
	originOff := e.Offset
	originAttempts := e.Attempts
	originEnqueued := e.Record.EnqueuedAt
	round := 1
	if prov, ok := ParseReplayProvenance(e.Record.Headers); ok {
		originTP = prov.OriginTopic + "-" + itoa(prov.OriginPartition)
		originOff = prov.OriginOffset
		originAttempts = prov.OriginAttempts
		originEnqueued = prov.OriginEnqueued
		round = prov.Round + 1
	}
	h[replayHeaderID] = opID
	h[replayHeaderGroup] = e.Group
	h[replayHeaderOriginTP] = originTP
	h[replayHeaderOriginOff] = itoa64(originOff)
	h[replayHeaderAttempts] = itoa(originAttempts)
	h[replayHeaderReasons] = strings.Join(e.Reasons, "\n")
	h[replayHeaderEnqueued] = strconv.FormatInt(originEnqueued.UnixNano(), 10)
	h[replayHeaderDeadAt] = strconv.FormatInt(e.DeadAt.UnixNano(), 10)
	h[replayHeaderRound] = itoa(round)
	return Message{
		Key:     append([]byte(nil), e.Record.Key...),
		Value:   append([]byte(nil), e.Record.Value...),
		Headers: h,
	}
}

// ParseReplayProvenance 从消息头解析重投溯源信息；非重投消息（或头损坏）返回 false。
func ParseReplayProvenance(h map[string]string) (ReplayProvenance, bool) {
	if len(h) == 0 {
		return ReplayProvenance{}, false
	}
	opID, ok := h[replayHeaderID]
	if !ok || opID == "" {
		return ReplayProvenance{}, false
	}
	p := ReplayProvenance{OpID: opID, Group: h[replayHeaderGroup]}
	tp, ok := parseTPKey(h[replayHeaderOriginTP])
	if !ok {
		return ReplayProvenance{}, false
	}
	p.OriginTopic, p.OriginPartition = tp.Topic, tp.Partition
	n, err := strconv.ParseInt(h[replayHeaderOriginOff], 10, 64)
	if err != nil {
		return ReplayProvenance{}, false
	}
	p.OriginOffset = n
	if v, err := strconv.Atoi(h[replayHeaderAttempts]); err == nil {
		p.OriginAttempts = v
	}
	if r := h[replayHeaderReasons]; r != "" {
		p.OriginReasons = strings.Split(r, "\n")
	}
	if v, err := strconv.ParseInt(h[replayHeaderEnqueued], 10, 64); err == nil {
		p.OriginEnqueued = time.Unix(0, v)
	}
	if v, err := strconv.ParseInt(h[replayHeaderDeadAt], 10, 64); err == nil {
		p.DeadAt = time.Unix(0, v)
	}
	if v, err := strconv.Atoi(h[replayHeaderRound]); err == nil {
		p.Round = v
	}
	return p, true
}
