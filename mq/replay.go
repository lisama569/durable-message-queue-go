package mq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// 重投来源使用保留消息头随分区日志持久化（日志记录格式无需变更），
// 因此重启后来源信息、再次失败的原因链累积都不依赖额外内存状态。
const (
	headerReplayID       = "x-mq-replay-id"
	headerReplayHop      = "x-mq-replay-hop"
	headerOriginGroup    = "x-mq-origin-group"
	headerOriginTP       = "x-mq-origin-tp"
	headerOriginOffset   = "x-mq-origin-offset"
	headerOriginAttempts = "x-mq-origin-attempts"
	headerOriginReasons  = "x-mq-origin-reasons" // JSON 数组
	headerOriginDeadAt   = "x-mq-origin-dead-at"
	headerOriginEnqueued = "x-mq-origin-enqueued-at"
)

// ReplayFilter 描述从某消费组死信中挑选重投消息的条件；零值字段表示不过滤。
type ReplayFilter struct {
	Topic         string    // 仅匹配该主题；为空表示全部主题
	Partitions    []int     // 仅匹配这些分区号；为空表示全部分区
	Offsets       []int64   // 精确指定死信原始位移；为空不按位移筛选
	DeadAtFrom    time.Time // 死信产生时间下界（含）；零值表示不限
	DeadAtTo      time.Time // 死信产生时间上界（含）；零值表示不限
	FailureReason string    // 失败原因子串（匹配历次原因链任一条）；为空不按原因筛选
}

// ReplayStatus 表示一次死信重投请求的生命周期状态。
type ReplayStatus string

const (
	ReplayPending   ReplayStatus = "pending"   // 已登记，尚未/未全部完成（含崩溃恢复后续跑）
	ReplayCompleted ReplayStatus = "completed" // 全部死信已作为新记录追加完成
)

// ReplayItem 描述一条死信的重投结果。
type ReplayItem struct {
	Topic     string
	Partition int
	// OldOffset 是死信消息在分区日志里的原始位移。
	OldOffset int64
	// NewOffset 是重投追加产生的新位移；尚未追加时为 -1。
	NewOffset int64
}

// ReplayResult 是一次重投请求的对外视图。
type ReplayResult struct {
	RequestID string
	Group     string
	Status    ReplayStatus
	CreatedAt time.Time
	Items     []ReplayItem
}

// ReplayOrigin 保留一条重投消息“上一跳进死信前”的完整来源信息，便于事后追溯。
type ReplayOrigin struct {
	RequestID       string    `json:"request_id"`
	Group           string    `json:"group"`
	Topic           string    `json:"topic"`
	Partition       int       `json:"partition"`
	Offset          int64     `json:"offset"`
	Attempts        int       `json:"attempts"`
	Reasons         []string  `json:"reasons"`
	DeadAt          time.Time `json:"dead_at"`
	FirstEnqueuedAt time.Time `json:"first_enqueued_at"`
	// Hop 为重投链跳数：1 表示第一次重投，依次递增。
	Hop int `json:"hop"`
}

func (o ReplayOrigin) tp() TopicPartition { return TopicPartition{o.Topic, o.Partition} }

// replayRequest 是持久化的重投请求记录。
type replayRequest struct {
	RequestID string            `json:"request_id"`
	Group     string            `json:"group"`
	CreatedAt time.Time         `json:"created_at"`
	Status    ReplayStatus      `json:"status"`
	Items     []replayItemState `json:"items"`
}

// replayItemState 是单条死信的重投进度。同一分区内条目按稳定顺序追加，
// 新位移由 Base 与条目在该分区内的序号确定性换算（见 broker）。
type replayItemState struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	OldOffset int64  `json:"old_offset"`
	// Base 是该分区中本请求追加动作开始时的日志末尾位移。
	Base int64 `json:"base"`
	// Appended 标记新记录是否已追加并 fsync 完成。
	Appended  bool  `json:"appended"`
	NewOffset int64 `json:"new_offset"`
}

func (i replayItemState) tp() TopicPartition { return TopicPartition{i.Topic, i.Partition} }

// replayState 是持久化到磁盘的整体快照。
type replayState struct {
	Seq      int64                      `json:"seq"`
	Requests map[string][]replayRequest `json:"requests"` // group -> requests
}

// replayStore 持久化重投请求（单文件原子写），线程安全。
type replayStore struct {
	mu       sync.Mutex
	path     string
	seq      int64
	requests map[string][]replayRequest
}

func replayFilePath(dir string) string {
	return filepath.Join(dir, "replays.json")
}

func newReplayStore(dir string) (*replayStore, error) {
	path := replayFilePath(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &replayStore{path: path, requests: map[string][]replayRequest{}}
	data, err := os.ReadFile(path)
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
		s.seq = st.Seq
		if st.Requests != nil {
			s.requests = st.Requests
		}
	}
	return s, nil
}

// nextID 生成进程内单调、重启后也不与历史请求冲突的确定性请求 ID。
func (s *replayStore) nextID(now time.Time) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextIDLocked(now)
}

// nextIDLocked 在锁内生成请求 ID，调用方须持有 s.mu。
func (s *replayStore) nextIDLocked(now time.Time) string {
	s.seq++
	return "rp-" + strconv.FormatInt(now.UnixNano(), 36) + "-" + strconv.FormatInt(s.seq, 10)
}

// create 登记一个新请求并持久化（先落盘再执行追加，保证崩溃可恢复）。
func (s *replayStore) create(req replayRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.requests[req.Group]
	for _, r := range list {
		if r.RequestID == req.RequestID {
			return ErrInvalidArgument
		}
	}
	s.requests[req.Group] = append(list, req)
	return s.persistLocked()
}

// get 读取某组的指定请求。
func (s *replayStore) get(group, requestID string) (replayRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.requests[group] {
		if r.RequestID == requestID {
			return r, true
		}
	}
	return replayRequest{}, false
}

// list 返回某组全部请求，按登记顺序排列。
func (s *replayStore) list(group string) []replayRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]replayRequest(nil), s.requests[group]...)
}

// update 覆盖写回某组的指定请求。
func (s *replayStore) update(req replayRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.requests[req.Group]
	for i := range list {
		if list[i].RequestID == req.RequestID {
			list[i] = req
			return s.persistLocked()
		}
	}
	return ErrReplayNotFound
}

// pending 返回所有组中尚未完成的请求（崩溃恢复后续跑用）。
func (s *replayStore) pending() []replayRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []replayRequest
	for _, list := range s.requests {
		for _, r := range list {
			if r.Status == ReplayPending {
				out = append(out, r)
			}
		}
	}
	return out
}

// ---- 重投来源与保留消息头之间的编解码 ----

func (s *replayStore) persistLocked() error {
	st := replayState{Seq: s.seq, Requests: s.requests}
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

// encodeOriginHeaders 把上一跳来源写入新记录的消息头（附加而非替换用户头）。
func encodeOriginHeaders(h map[string]string, o ReplayOrigin) map[string]string {
	out := cloneHeaders(h)
	if out == nil {
		out = map[string]string{}
	}
	out[headerReplayID] = o.RequestID
	out[headerReplayHop] = strconv.Itoa(o.Hop)
	out[headerOriginGroup] = o.Group
	out[headerOriginTP] = o.tp().String()
	out[headerOriginOffset] = strconv.FormatInt(o.Offset, 10)
	out[headerOriginAttempts] = strconv.Itoa(o.Attempts)
	if raw, err := json.Marshal(o.Reasons); err == nil {
		out[headerOriginReasons] = string(raw)
	}
	out[headerOriginDeadAt] = strconv.FormatInt(o.DeadAt.UnixNano(), 10)
	out[headerOriginEnqueued] = strconv.FormatInt(o.FirstEnqueuedAt.UnixNano(), 10)
	return out
}

// originFromHeaders 从记录消息头解析重投来源；不存在重投头则返回 nil。
func originFromHeaders(h map[string]string) *ReplayOrigin {
	if len(h) == 0 || h[headerReplayID] == "" {
		return nil
	}
	o := &ReplayOrigin{
		RequestID: h[headerReplayID],
		Group:     h[headerOriginGroup],
		Offset:    atoi64(h[headerOriginOffset]),
		Attempts:  atoi(h[headerOriginAttempts]),
		Hop:       atoi(h[headerReplayHop]),
		DeadAt:    time.Unix(0, atoi64(h[headerOriginDeadAt])),
	}
	o.FirstEnqueuedAt = time.Unix(0, atoi64(h[headerOriginEnqueued]))
	if tp, ok := parseTPKey(h[headerOriginTP]); ok {
		o.Topic, o.Partition = tp.Topic, tp.Partition
	}
	if raw := h[headerOriginReasons]; raw != "" {
		_ = json.Unmarshal([]byte(raw), &o.Reasons)
	}
	return o
}

// ReplayInfo 从投递记录的保留消息头解析重投来源；非重投消息返回 nil。
func (d Delivery) ReplayInfo() *ReplayOrigin {
	return originFromHeaders(d.Record.Headers)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
