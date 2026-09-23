package mq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Clock 抽象时间来源，生产用系统时钟，测试用可控时钟。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// ownership 表示某个分区在某一代（generation）上被某个消费者独占。
type ownership struct {
	ConsumerID string    `json:"consumer_id"`
	Generation int64     `json:"generation"`
	Deadline   time.Time `json:"deadline"`
}

type member struct {
	id            string
	gen           int64 // 该会话注册时的代次令牌
	lastHeartbeat time.Time
}

// Consumer 是消费者句柄；generation 用于再平衡后的栅栏隔离：
// 一旦发生再平衡，旧代次的 commit/poll 都会被 ErrStaleGeneration 拒绝。
type Consumer struct {
	id     string
	group  *group
	gen    int64
	closed bool
}

func (c *Consumer) ID() string { return c.id }

// groupState 是持久化到磁盘的消费组位移快照。
// 成员与独占关系是租约态，进程重启后一律失效（需要重新加入并分配），
// 因此只有 generation 与 committed 位移落盘。
type groupState struct {
	Generation int64            `json:"generation"`
	Committed  map[string]int64 `json:"committed"` // tp.String() -> offset
}

// group 管理成员、心跳租约、分区独占关系与已提交位移（持久化）。
type group struct {
	mu         sync.Mutex
	name       string
	path       string
	clock      Clock
	leaseTTL   time.Duration
	generation int64
	members    map[string]*member
	committed  map[TopicPartition]int64
	owners     map[TopicPartition]ownership
}

func groupFilePath(dir, name string) string {
	return filepath.Join(dir, "groups", name+".json")
}

func newGroup(name, dir string, leaseTTL time.Duration, clock Clock) (*group, error) {
	if name == "" {
		return nil, ErrInvalidArgument
	}
	path := groupFilePath(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	g := &group{
		name:      name,
		path:      path,
		clock:     clock,
		leaseTTL:  leaseTTL,
		members:   map[string]*member{},
		committed: map[TopicPartition]int64{},
		owners:    map[TopicPartition]ownership{},
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		var st groupState
		if len(data) > 0 {
			if err := json.Unmarshal(data, &st); err != nil {
				return nil, err
			}
			g.generation = st.Generation
			for k, v := range st.Committed {
				tp, ok := parseTPKey(k)
				if !ok {
					continue
				}
				g.committed[tp] = v
			}
		}
	case os.IsNotExist(err):
		// 首次创建
	default:
		return nil, err
	}
	return g, nil
}

// tpKey 的编码与 TopicPartition.String 一致：topic-partition。
func tpKey(tp TopicPartition) string { return tp.String() }

func parseTPKey(s string) (TopicPartition, bool) {
	i := -1
	// 找最后一个 '-'，因为 topic 名可能含 '-'。
	for j := len(s) - 1; j >= 0; j-- {
		if s[j] == '-' {
			i = j
			break
		}
	}
	if i < 0 {
		return TopicPartition{}, false
	}
	n, err := parseInt(s[i+1:])
	if err != nil {
		return TopicPartition{}, false
	}
	return TopicPartition{Topic: s[:i], Partition: n}, true
}

func parseInt(s string) (int, error) {
	if s == "" {
		return 0, ErrInvalidArgument
	}
	neg := false
	i := 0
	if s[0] == '-' {
		neg = true
		i = 1
	}
	n := 0
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, ErrInvalidArgument
		}
		n = n*10 + int(s[i]-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}

func (g *group) register(id string) (*Consumer, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if id == "" {
		return nil, ErrInvalidArgument
	}
	if _, exists := g.members[id]; exists {
		return nil, ErrConsumerRegistered
	}
	now := g.clock.Now()
	g.members[id] = &member{id: id, gen: g.generation, lastHeartbeat: now}
	// 成员加入不推进全局代次：现有持有者的分区不被撤销，
	// 新成员只参与后续无主分区的分配。代次仅在成员离开/过期时推进，
	// 但比较的是“会话令牌”（成员注册时的代次），因此再平衡只隔离
	// 离开/过期的那个会话，不波及其余存活成员。
	c := &Consumer{id: id, group: g, gen: g.generation}
	if err := g.persistLocked(); err != nil {
		delete(g.members, id)
		return nil, err
	}
	return c, nil
}

func (c *Consumer) heartbeat() error {
	return c.group.heartbeat(c.id)
}

func (g *group) heartbeat(id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.members[id]
	if !ok {
		return ErrUnknownConsumer
	}
	m.lastHeartbeat = g.clock.Now()
	return nil
}

// leave 主动退出消费组：释放其持有的全部分区，并推进代次。
func (g *group) leave(c *Consumer) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.members[c.id]; !ok {
		return
	}
	c.closed = true
	delete(g.members, c.id)
	for tp, o := range g.owners {
		if o.ConsumerID == c.id {
			delete(g.owners, tp)
		}
	}
	g.generation++
	_ = g.persistLocked()
}

// expireLeases 清除心跳超时的成员并释放其分区；成员集合变化时推进代次。
// 返回（是否发生再平衡, 当前代次）。
func (g *group) expireLeases(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	changed := false
	for id, m := range g.members {
		if now.Sub(m.lastHeartbeat) > g.leaseTTL {
			delete(g.members, id)
			changed = true
			for tp, o := range g.owners {
				if o.ConsumerID == id {
					delete(g.owners, tp)
				}
			}
		}
	}
	if changed {
		g.generation++
		_ = g.persistLocked()
	}
	return changed
}

// sortedMembers 返回当前存活成员的稳定排序，保证分配确定可复现。
func (g *group) sortedMembersLocked() []string {
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// assignLocked 按确定性规则把无主分区分配给存活成员：
// 对每个分区按成员数取模。已经合法持有的分区保持不动（粘滞）。
func (g *group) assignLocked(allTP []TopicPartition, now time.Time) {
	alive := g.sortedMembersLocked()
	if len(alive) == 0 {
		return
	}
	for _, tp := range allTP {
		if o, ok := g.owners[tp]; ok {
			if m, exists := g.members[o.ConsumerID]; exists &&
				now.Sub(m.lastHeartbeat) <= g.leaseTTL {
				continue // 现有持有者仍有效，保持粘滞
			}
			delete(g.owners, tp)
		}
		idx := partitionIndex(tp) % len(alive)
		g.owners[tp] = ownership{
			ConsumerID: alive[idx],
			Generation: g.generation,
			Deadline:   now.Add(g.leaseTTL),
		}
	}
}

// partitionIndex 用分区号作为确定性分配键（同主题下已足够稳定）。
func partitionIndex(tp TopicPartition) int {
	if tp.Partition < 0 {
		return -tp.Partition
	}
	return tp.Partition
}

// ensureOwner 确保 c 在当前代持有 tp（懒分配），并刷新租约截止时间。
// 代次过期 / 分区被他人独占都会得到明确错误。
func (g *group) ensureOwner(c *Consumer, tp TopicPartition, allTP []TopicPartition, now time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.members[c.id]
	if !ok {
		return ErrUnknownConsumer
	}
	if c.gen != m.gen {
		return ErrStaleGeneration
	}
	if now.Sub(m.lastHeartbeat) > g.leaseTTL {
		return ErrLeaseExpired
	}
	if o, owned := g.owners[tp]; owned {
		if o.ConsumerID != c.id {
			return ErrPartitionOwned
		}
		// 持有者就是自己：粘滞保留；是否过期由上面的成员心跳判断。
	}
	if _, owned := g.owners[tp]; !owned {
		g.assignLocked(allTP, now)
	}
	o, owned := g.owners[tp]
	if !owned || o.ConsumerID != c.id {
		return ErrPartitionOwned
	}
	o.Deadline = now.Add(g.leaseTTL)
	g.owners[tp] = o
	return nil
}

// checkOwner 校验 c 当前仍合法持有 tp（租约 + 代次栅栏）。
func (g *group) checkOwner(c *Consumer, tp TopicPartition, now time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.members[c.id]
	if !ok {
		return ErrUnknownConsumer
	}
	if c.gen != m.gen {
		return ErrStaleGeneration
	}
	o, ok := g.owners[tp]
	if !ok {
		return ErrNotOwner
	}
	if o.ConsumerID != c.id {
		return ErrNotOwner
	}
	if now.After(o.Deadline) {
		return ErrLeaseExpired
	}
	return nil
}

func (g *group) committedOffset(tp TopicPartition) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.committed[tp]
}

// commit 幂等提交位移（语义：offset 为“下一条待消费位移”）。
//   - 与已提交值相同 => 幂等成功；
//   - 小于已提交值 => ErrOffsetRolledBack；
//   - 大于日志末尾 => ErrOffsetOutOfRange；
//   - 调用者非当前合法持有者 => 对应租约/代次错误。
func (g *group) commit(c *Consumer, tp TopicPartition, offset, logEnd int64, now time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.members[c.id]
	if !ok {
		return ErrUnknownConsumer
	}
	if c.gen != m.gen {
		return ErrStaleGeneration
	}
	o, ok := g.owners[tp]
	if !ok || o.ConsumerID != c.id {
		return ErrNotOwner
	}
	if now.After(o.Deadline) {
		return ErrLeaseExpired
	}
	if offset < 0 {
		return ErrOffsetOutOfRange
	}
	if offset > logEnd {
		return ErrOffsetOutOfRange
	}
	cur := g.committed[tp]
	if offset < cur {
		return ErrOffsetRolledBack
	}
	if offset == cur {
		return nil // 幂等提交
	}
	g.committed[tp] = offset
	return g.persistLocked()
}

// advanceCommit 由 Broker 在“连续确认”时推进位移；同样做回退/越界保护。
func (g *group) advanceCommit(c *Consumer, tp TopicPartition, offset, logEnd int64, now time.Time) error {
	return g.commit(c, tp, offset, logEnd, now)
}

func (g *group) assignments() map[TopicPartition]string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[TopicPartition]string, len(g.owners))
	for tp, o := range g.owners {
		out[tp] = o.ConsumerID
	}
	return out
}

func (g *group) currentGen() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation
}

func (g *group) persistLocked() error {
	st := groupState{
		Generation: g.generation,
		Committed:  map[string]int64{},
	}
	for tp, off := range g.committed {
		st.Committed[tpKey(tp)] = off
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := g.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, g.path)
}
