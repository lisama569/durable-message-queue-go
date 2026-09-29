package mq

import (
	"log"
	"sort"
	"sync"
	"time"
)

// Config 控制 Broker 行为。
type Config struct {
	DataDir      string        // 本地持久化目录
	LeaseTTL     time.Duration // 消费者心跳租约；超时视为崩溃，分区可被再分配
	PartCapacity int           // 每个分区的容量，达到后 Produce 返回 ErrQueueFull
	MaxInflight  int           // 每个（消费组×分区）允许的在途消息数
	Retry        RetryPolicy
	Clock        Clock
	Logger       *log.Logger
}

// DefaultConfig：10s 租约、10000 条/分区、8 条在途窗口。
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:      dataDir,
		LeaseTTL:     10 * time.Second,
		PartCapacity: 10000,
		MaxInflight:  8,
		Retry:        DefaultRetryPolicy(),
		Clock:        systemClock{},
	}
}

type gt struct {
	group string
	tp    TopicPartition
}

// inflight 描述已投递但尚未确认的消息。
type inflight struct {
	delivery Delivery
}

// Broker 是本地持久化消息队列的入口，线程安全。
type Broker struct {
	cfg      Config
	clock    Clock
	logger   *log.Logger
	mu       sync.Mutex
	logs     map[TopicPartition]*PartitionLog
	tps      []TopicPartition
	groups   map[string]*group
	retries  *retryStore
	replays  *replayStore
	inflight map[gt]map[int64]*inflight
	// deliveredHi[gt] = 该组×分区“已投递过”的连续上界（下一条从未投递的位移）。
	// 提交位绝不允许越过它：否则未投递消息会被静默跳过。
	// 进程重启或分区易主导致在途回收时，游标回退到已提交位，
	// 未确认消息将被重新投递（至少一次语义）。
	deliveredHi map[gt]int64
	seq         int64
}

// NewBroker 打开/恢复一个本地持久化队列。
func NewBroker(cfg Config) (*Broker, error) {
	if cfg.DataDir == "" {
		return nil, ErrInvalidArgument
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultConfig(cfg.DataDir).LeaseTTL
	}
	if cfg.PartCapacity <= 0 {
		cfg.PartCapacity = DefaultConfig(cfg.DataDir).PartCapacity
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = DefaultConfig(cfg.DataDir).MaxInflight
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry = DefaultRetryPolicy()
	}
	if cfg.Clock == nil {
		cfg.Clock = systemClock{}
	}
	rs, err := newRetryStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	rps, err := newReplayStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:         cfg,
		clock:       cfg.Clock,
		logger:      cfg.Logger,
		logs:        map[TopicPartition]*PartitionLog{},
		groups:      map[string]*group{},
		retries:     rs,
		replays:     rps,
		inflight:    map[gt]map[int64]*inflight{},
		deliveredHi: map[gt]int64{},
	}
	b.replays.reconcile(rs)
	return b, nil
}

func (b *Broker) logf(format string, args ...any) {
	if b.logger != nil {
		b.logger.Printf(format, args...)
	}
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, l := range b.logs {
		_ = l.close()
	}
	return nil
}

// CreateTopic 预创建分区日志文件。
func (b *Broker) CreateTopic(topic string, partitions int) error {
	if topic == "" || partitions <= 0 {
		return ErrInvalidArgument
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < partitions; i++ {
		tp := TopicPartition{Topic: topic, Partition: i}
		if _, ok := b.logs[tp]; ok {
			continue
		}
		l, err := newPartitionLog(tp, b.cfg.DataDir, b.cfg.PartCapacity)
		if err != nil {
			return err
		}
		b.logs[tp] = l
		b.tps = append(b.tps, tp)
	}
	sort.Slice(b.tps, func(i, j int) bool {
		if b.tps[i].Topic != b.tps[j].Topic {
			return b.tps[i].Topic < b.tps[j].Topic
		}
		return b.tps[i].Partition < b.tps[j].Partition
	})
	return nil
}

func (b *Broker) getLogLocked(tp TopicPartition) (*PartitionLog, bool) {
	l, ok := b.logs[tp]
	return l, ok
}

// Produce 向分区尾部顺序追加一条消息（先持久化再返回，保证不丢）。
func (b *Broker) Produce(tp TopicPartition, m Message) (int64, error) {
	b.mu.Lock()
	l, ok := b.logs[tp]
	b.mu.Unlock()
	if !ok {
		return 0, ErrPartitionNotFound
	}
	rec, err := l.Append(m)
	if err != nil {
		return 0, err
	}
	b.logf("[produce] id=%s status=appended key=%q", rec.MessageID(), string(m.Key))
	return rec.Offset, nil
}

// ReadRecord 按位移读取记录（便于测试与重放）。
func (b *Broker) ReadRecord(tp TopicPartition, offset int64) (Record, bool) {
	b.mu.Lock()
	l, ok := b.logs[tp]
	b.mu.Unlock()
	if !ok {
		return Record{}, false
	}
	return l.Read(offset)
}

// JoinGroup 注册消费者并返回句柄。
func (b *Broker) JoinGroup(groupName, consumerID string) (*Consumer, error) {
	b.mu.Lock()
	g, err := b.getOrCreateGroupLocked(groupName)
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return g.register(consumerID)
}

func (b *Broker) getOrCreateGroupLocked(name string) (*group, error) {
	if g, ok := b.groups[name]; ok {
		return g, nil
	}
	g, err := newGroup(name, b.cfg.DataDir, b.cfg.LeaseTTL, b.clock)
	if err != nil {
		return nil, err
	}
	b.groups[name] = g
	return g, nil
}

// Heartbeat 续租。
func (b *Broker) Heartbeat(c *Consumer) error { return c.heartbeat() }

// LeaveGroup 主动退出；分区立即可被其他消费者接管。
func (b *Broker) LeaveGroup(c *Consumer) {
	b.mu.Lock()
	b.reclaimInflightLocked(c.group.name, c.id)
	b.mu.Unlock()
	c.group.leave(c)
}

// expire 检查所有组的过期租约，回收其在途消息（将被重新投递）。
func (b *Broker) expire(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for name, g := range b.groups {
		// 找出过期前的成员，回收其在途
		dead := b.expiredMembersLocked(g, now)
		if g.expireLeases(now) {
			for _, id := range dead {
				b.reclaimInflightLocked(name, id)
			}
			b.logf("[rebalance] group=%s gen=%d reason=lease-expired dead=%v",
				name, g.currentGen(), dead)
		}
	}
}

// expiredMembersLocked 在 expireLeases 清表前快照过期成员 id。
func (b *Broker) expiredMembersLocked(g *group, now time.Time) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var dead []string
	for id, m := range g.members {
		if now.Sub(m.lastHeartbeat) > g.leaseTTL {
			dead = append(dead, id)
		}
	}
	return dead
}

// reclaimInflightLocked 释放指定消费者名下的全部在途消息，
// 这些位移会在下一次 Poll 中被重新投递（不跳过、不静默）。
func (b *Broker) reclaimInflightLocked(groupName, consumerID string) {
	for k, set := range b.inflight {
		if k.group != groupName {
			continue
		}
		for off, inf := range set {
			if inf.delivery.ConsumerID == consumerID {
				delete(set, off)
				b.logf("[reclaim] group=%s id=%s#%s reason=owner-left",
					groupName, k.tp.String(), itoa64(off))
			}
		}
		if len(set) == 0 {
			delete(b.inflight, k)
		}
	}
	// 该组分区的在途消息已随旧主离开而失效：投递游标回退到已提交位，
	// 未确认位移将被重新投递（不跳过、不静默）。
	for k := range b.deliveredHi {
		if k.group == groupName {
			if g, ok := b.groups[groupName]; ok {
				b.deliveredHi[k] = g.committedOffset(k.tp)
			}
		}
	}
}

// Step 推进内部租约过期处理；测试可控时钟下每拍调用一次。
func (b *Broker) Step() {
	b.expire(b.clock.Now())
}

// Poll 拉取一批可消费的投递：
//  1. 清理过期租约；
//  2. 对每个分区做独占分配校验，只有持有者能取到消息；
//  3. 从已提交位移顺序扫描，跳过在途与退避未到期的消息，
//     退避到期的重试消息优先投递；
//  4. 投递窗口受 MaxInflight 限制，形成反压而非丢弃。
func (b *Broker) Poll(c *Consumer, max int) ([]Delivery, error) {
	if max <= 0 {
		return nil, ErrInvalidArgument
	}
	now := b.clock.Now()
	b.expire(now)

	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[c.group.name]
	if !ok {
		return nil, ErrUnknownConsumer
	}

	var out []Delivery
	for _, tp := range b.tps {
		if len(out) >= max {
			break
		}
		l, exists := b.logs[tp]
		if !exists {
			continue
		}
		if err := g.ensureOwner(c, tp, b.tps, now); err != nil {
			// 非持有者/旧代次是正常情形：跳过该分区，不报错中断整批。
			continue
		}
		out = append(out, b.collectLocked(g.name, c, tp, l, max-len(out), now)...)
	}
	if len(out) > 0 {
		b.logf("[poll] group=%s consumer=%s delivered=%d first=%s last=%s",
			g.name, c.id, len(out), out[0].Record.MessageID(), out[len(out)-1].Record.MessageID())
	}
	return out, nil
}

// collectLocked 扫描单个分区，组装该消费者此刻可取的投递。
func (b *Broker) collectLocked(groupName string, c *Consumer, tp TopicPartition,
	l *PartitionLog, budget int, now time.Time) []Delivery {

	key := gt{group: groupName, tp: tp}
	set := b.inflight[key]
	if set == nil {
		set = map[int64]*inflight{}
		b.inflight[key] = set
	}

	g := b.groups[groupName]
	committed := g.committedOffset(tp)
	end := l.endOffset()
	// 投递游标：首次访问（或重启/接管回退后）从已提交位开始；正常推进中只增不减。
	hi, started := b.deliveredHi[key]
	if !started || hi < committed {
		hi = committed
		b.deliveredHi[key] = hi
	}
	var out []Delivery
	// 本轮扫描是否越过了死信终态位移；越过之后即便没有新投递，
	// 也要尝试把连续前缀提交位推过去，避免“为避开死信而卡住提交位”。
	terminalSkipped := false

	// 1) 先重新投递退避已到期的重试消息（其位移必然 < hi 且当前不在途）。
	for _, e := range b.retries.due(groupName, now) {
		if len(out) >= budget {
			break
		}
		if e.tp() != tp || e.Offset < committed || e.Offset >= end {
			continue
		}
		// 防御：终态死信绝不走重投路径（正常状态机下待重试与死信互斥）。
		if _, dead := b.retries.deadState(groupName, tp, e.Offset); dead {
			continue
		}
		if _, busy := set[e.Offset]; busy {
			continue
		}
		rec, exists := l.Read(e.Offset)
		if !exists {
			continue
		}
		d := b.deliverLocked(groupName, c, tp, rec, set, e.Attempt+1, now)
		out = append(out, d)
		b.logf("[poll-retry] id=%s attempt=%d reason=backoff-eligible",
			rec.MessageID(), d.Attempt)
	}

	// 2) 从投递游标 hi 起顺序首发新消息；遇到在途或退避未到期消息必须停住，
	//    保证“同分区顺序稳定”、不跳过未决消息。
	for off := hi; off < end && len(out) < budget; off++ {
		// 死信终态：该位移已经投递并失败到上限，持久化的死信记录即终态标记。
		// 任何重启、租约过期接管、游标回退到已提交位的场景下都只跳过、不投递；
		// 只有显式 ReplayDeadLetters 在日志尾部另立的【新记录】才会再被消费。
		// 游标随跳过一并推进：死信位移此前必然已经投递过，推进它不会把
		// 从未投递的消息静默跳掉。
		if _, dead := b.retries.deadState(groupName, tp, off); dead {
			b.logf("[poll-skip-dlq] id=%s#%s reason=dead-letter-terminal",
				tp.String(), itoa64(off))
			hi = off + 1
			b.deliveredHi[key] = hi
			terminalSkipped = true
			continue
		}
		if _, busy := set[off]; busy {
			break // 窗口内有未确认消息：保持顺序，停止首发推进
		}
		if e, waiting := b.retries.retryState(groupName, tp, off); waiting {
			if now.Before(e.NotBefore) {
				b.logf("[poll-blocked] id=%s#%s reason=retry-backoff not-before=%s",
					tp.String(), itoa64(off), e.NotBefore.Format(time.RFC3339Nano))
				break
			}
			// 到期情况已在上面处理，且它必已投递过（off < hi），不在此首发。
			break
		}
		// 投递窗口上限即反压信号：不丢弃，留到下次 Poll。
		if len(set) >= b.cfg.MaxInflight {
			b.logf("[poll-backpressure] group=%s tp=%s inflight=%d limit=%d offset=%s",
				groupName, tp.String(), len(set), b.cfg.MaxInflight, itoa64(off))
			break
		}
		rec, exists := l.Read(off)
		if !exists {
			break
		}
		d := b.deliverLocked(groupName, c, tp, rec, set, 1, now)
		out = append(out, d)
		hi = off + 1
		b.deliveredHi[key] = hi
	}

	// 重启/接管后可能出现：连续前缀里夹着此前已进死信的位移（其左侧的未确认
	// 消息刚刚在接管后被重新确认），而本轮没有新的 Ack 发生。越过终态后主动
	// 补一次连续前缀提交，保证提交位不会为了避开死信而永久卡住；前方仍有
	// 在途/待重试消息时该函数自然停在那里，不会越过未决消息。
	if terminalSkipped {
		b.advanceContiguousLocked(g, c, tp, now)
	}
	return out
}

func (b *Broker) deliverLocked(groupName string, c *Consumer, tp TopicPartition,
	rec Record, set map[int64]*inflight, attempt int, now time.Time) Delivery {
	b.seq++
	d := Delivery{
		Record:      rec,
		Group:       groupName,
		ConsumerID:  c.id,
		DeliverySeq: b.seq,
		Attempt:     attempt,
		DeliveredAt: now,
	}
	set[rec.Offset] = &inflight{delivery: d}
	return d
}

// Ack 幂等确认一条投递成功：移除在途/待重试状态，若确认的是连续前缀
// 则幂等推进已提交位移。重复 Ack 同一位移返回 nil（幂等），
// 但代次过期 / 非持有者会被明确拒绝。
func (b *Broker) Ack(c *Consumer, d Delivery) error {
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	g, ok := b.groups[d.Group]
	if !ok {
		return ErrUnknownConsumer
	}
	tp := d.Record.topicPartition()
	if err := g.checkOwner(c, tp, now); err != nil {
		return err
	}
	key := gt{group: d.Group, tp: tp}
	set := b.inflight[key]

	// 幂等：位移既不在途、也不在待重试，说明此前已确认过。
	_, busy := set[d.Record.Offset]
	_, retrying := b.retries.retryState(d.Group, tp, d.Record.Offset)
	if !busy && !retrying {
		b.logf("[ack] id=%s status=idempotent committed=%s reason=already-acked",
			d.Record.MessageID(), itoa64(g.committedOffset(tp)))
		return nil
	}
	delete(set, d.Record.Offset)
	b.retries.remove(d.Group, tp, d.Record.Offset)
	if len(set) == 0 {
		delete(b.inflight, key)
	}

	// 连续前缀推进：从 committed 开始，跳过已被确认（既不在途也不待重试）
	// 且位移连续的记录，直到遇到下一个未决消息。
	committed := b.advanceContiguousLocked(g, c, tp, now)
	b.logf("[ack] id=%s status=acked attempt=%d committed=%s",
		d.Record.MessageID(), d.Attempt, itoa64(committed))
	return nil
}

// advanceContiguousLocked 计算连续已确认前缀并提交。
// 上界为 deliveredHi：从未投递过的位移绝不允许计入提交。
func (b *Broker) advanceContiguousLocked(g *group, c *Consumer,
	tp TopicPartition, now time.Time) int64 {
	l, ok := b.logs[tp]
	if !ok {
		return g.committedOffset(tp)
	}
	key := gt{group: g.name, tp: tp}
	limit := l.endOffset()
	if hi, started := b.deliveredHi[key]; started && hi < limit {
		limit = hi
	}
	set := b.inflight[key]
	off := g.committedOffset(tp)
	for off < limit {
		if _, busy := set[off]; busy {
			break
		}
		if _, waiting := b.retries.retryState(g.name, tp, off); waiting {
			break
		}
		off++
	}
	if off > g.committedOffset(tp) {
		if err := g.advanceCommit(c, tp, off, l.endOffset(), now); err != nil {
			b.logf("[commit] tp=%s offset=%s status=rejected err=%v",
				tp.String(), itoa64(off), err)
		}
	}
	return off
}

// Nack 声明处理失败：
//   - 先从在途移除（分区不再被该消息阻塞）；
//   - 按确定性退避登记重试；
//   - 达到 MaxAttempts 则消息（含快照与原因链）进入死信，
//     该位移视为终态，连续前缀可继续推进。
func (b *Broker) Nack(c *Consumer, d Delivery, reason string) error {
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	g, ok := b.groups[d.Group]
	if !ok {
		return ErrUnknownConsumer
	}
	tp := d.Record.topicPartition()
	if err := g.checkOwner(c, tp, now); err != nil {
		return err
	}
	key := gt{group: d.Group, tp: tp}
	if set := b.inflight[key]; set != nil {
		delete(set, d.Record.Offset)
		if len(set) == 0 {
			delete(b.inflight, key)
		}
	}

	entry := retryEntry{
		Group:      d.Group,
		Topic:      tp.Topic,
		Partition:  tp.Partition,
		Offset:     d.Record.Offset,
		Attempt:    d.Attempt,
		LastReason: reason,
	}
	err := b.retries.schedule(entry, d.Record, b.cfg.Retry, now)
	switch err {
	case ErrRetryExhausted:
		b.logf("[nack] id=%s status=dead-lettered attempts=%d reason=%q",
			d.Record.MessageID(), d.Attempt, reason)
		// 死信为终态：尝试推进连续前缀提交。
		committed := b.advanceContiguousLocked(g, c, tp, now)
		b.logf("[commit-after-dlq] id=%s committed=%s", d.Record.MessageID(), itoa64(committed))
		return ErrRetryExhausted
	case nil:
		b.logf("[nack] id=%s status=retry-scheduled attempt=%d backoff=%s reason=%q",
			d.Record.MessageID(), d.Attempt,
			b.cfg.Retry.Backoff(d.Attempt).String(), reason)
		return nil
	default:
		return err
	}
}

// CommitOffset 显式幂等提交位移。
func (b *Broker) CommitOffset(c *Consumer, tp TopicPartition, offset int64) error {
	now := b.clock.Now()
	b.mu.Lock()
	g, ok := b.groups[c.group.name]
	if !ok {
		b.mu.Unlock()
		return ErrUnknownConsumer
	}
	l, exists := b.logs[tp]
	if !exists {
		b.mu.Unlock()
		return ErrPartitionNotFound
	}
	logEnd := l.endOffset()
	b.mu.Unlock()

	err := g.commit(c, tp, offset, logEnd, now)
	if err != nil {
		b.logf("[commit] tp=%s offset=%s status=rejected committed=%s logEnd=%s err=%v",
			tp.String(), itoa64(offset), itoa64(g.committedOffset(tp)),
			itoa64(logEnd), err)
		return err
	}
	b.logf("[commit] tp=%s offset=%s status=ok", tp.String(), itoa64(offset))
	return nil
}

// LogStats 返回所有分区的容量水位（可观测）。
func (b *Broker) LogStats() []LogStats {
	b.mu.Lock()
	tps := append([]TopicPartition(nil), b.tps...)
	logs := make(map[TopicPartition]*PartitionLog, len(b.logs))
	for tp, l := range b.logs {
		logs[tp] = l
	}
	b.mu.Unlock()
	out := make([]LogStats, 0, len(tps))
	for _, tp := range tps {
		out = append(out, logs[tp].Stats())
	}
	return out
}

// Assignments 返回某消费组当前的分区独占关系。
func (b *Broker) Assignments(groupName string) map[TopicPartition]string {
	b.mu.Lock()
	g, ok := b.groups[groupName]
	b.mu.Unlock()
	if !ok {
		return nil
	}
	return g.assignments()
}

// DeadLetters 读取某组的死信记录。
func (b *Broker) DeadLetters(groupName string) []DLQEntry {
	return b.retries.deadLetters(groupName)
}

// ReplayDeadLetters 触发某消费组的死信重投（按 filter 筛选）。
// idemKey 为调用方提供的幂等键：相同 (group, idemKey) 的重复调用、以及
// 进程重启后的再次调用，只会恢复并继续同一个操作，绝不重复入队。
//
// 重投消息以【新记录】追加到同分区日志尾部，因此：
//   - 已提交/已确认的其他消息（含原死信位移）绝不会被再投一遍，位移不回退；
//   - 重投消息天然排在同分区所有现有消息之后，后续消息不会跑到它前面；
//   - 它像正常消息一样被该组消费，并重新获得完整的 MaxAttempts 重试机会；
//   - 原始来源、失败次数与完整原因链保留在消息头（见 ParseReplayProvenance）。
//
// 受分区容量反压：装不下时返回 ErrQueueFull，已入队部分不回滚，调用方稍后
// 用 ResumeReplay 续跑；不会静默丢弃，也不会重复入队。
func (b *Broker) ReplayDeadLetters(groupName, idemKey string, filter ReplayFilter) (ReplayResult, error) {
	if groupName == "" {
		return ReplayResult{}, ErrInvalidArgument
	}
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	if idemKey == "" {
		b.seq++
		idemKey = "auto-" + newOpID(now, b.seq)
	}

	// 幂等：同组同键重复/重启后调用 -> 恢复并继续既有操作。
	if op, ok := b.replays.findByIdempotency(groupName, idemKey); ok {
		b.logf("[replay] group=%s key=%s status=resume-existing op=%s", groupName, idemKey, op.OpID)
		return b.resumeReplayLocked(op.OpID)
	}

	dead := b.retries.deadLetters(groupName)
	candidates := selectReplayCandidates(dead, filter)
	res := ReplayResult{Matched: len(candidates)}
	if len(candidates) == 0 {
		b.logf("[replay] group=%s key=%s status=empty reason=no-match", groupName, idemKey)
		return res, ErrNothingToReplay
	}

	b.seq++
	op, claimed := b.replays.createReplay(b.retries, groupName, idemKey, candidates, now, b.seq)
	if snap, ok := b.replays.getOp(op.OpID); ok {
		res.Operation = snap
	}
	b.logf("[replay] op=%s group=%s key=%s matched=%d claimed=%d status=created",
		op.OpID, groupName, idemKey, len(candidates), len(claimed))

	enqueued, already, runErr := b.runReplayLocked(op.OpID)
	res.EnqueuedNow = enqueued
	res.AlreadyDone = already
	if snap, ok := b.replays.getOp(op.OpID); ok {
		res.Operation = snap
	}
	return res, runErr
}

// ResumeReplay 继续推进一个未完成的重投操作（容量恢复后调用，或重启后续跑）。
func (b *Broker) ResumeReplay(opID string) (ReplayResult, error) {
	if opID == "" {
		return ReplayResult{}, ErrInvalidArgument
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.resumeReplayLocked(opID)
}

func (b *Broker) resumeReplayLocked(opID string) (ReplayResult, error) {
	op, ok := b.replays.getOp(opID)
	if !ok {
		return ReplayResult{}, ErrReplayNotFound
	}
	res := ReplayResult{Operation: op, Matched: len(op.Items)}
	enqueued, already, err := b.runReplayLocked(opID)
	res.EnqueuedNow = enqueued
	res.AlreadyDone = already
	if snap, ok := b.replays.getOp(opID); ok {
		res.Operation = snap
	}
	return res, err
}

// runReplayLocked 串行推进操作中所有未完成条目。
// 返回 (本次新确认入队数, 恢复时发现此前已入队数, 首个阻塞错误)。
// 所有重投入队在此按 (分区, 原始位移) 顺序串行完成，保证同分区顺序确定。
func (b *Broker) runReplayLocked(opID string) (int, int, error) {
	now := b.clock.Now()
	op, ok := b.replays.getOp(opID)
	if !ok {
		return 0, 0, ErrReplayNotFound
	}
	enqueuedNow, alreadyDone := 0, 0
	for idx := range op.Items {
		item := op.Items[idx]
		if item.Status == ReplayComplete {
			alreadyDone++
			continue
		}
		tp := item.tp()
		l, exists := b.logs[tp]
		if !exists {
			b.replays.markItemError(opID, item, ErrPartitionNotFound.Error(), b.clock.Now())
			return enqueuedNow, alreadyDone, ErrPartitionNotFound
		}

		// 幂等探测：扫描扫描起点之后的日志，若本操作的重投副本此前已写入
		// （写入后、标记前崩溃），直接认领该记录，绝不重复追加。
		if _, newOff, found := b.findReplayRecordLocked(l, opID, tp, item.OriginOffset, item.ScanFrom); found {
			b.retries.markReplayedEnqueued(op.Group, opID, tp, item.OriginOffset, newOff, b.clock.Now())
			done := ReplayItem(item)
			done.Status = ReplayComplete
			done.NewOffset = newOff
			b.replays.markEnqueued(opID, done, b.clock.Now())
			alreadyDone++
			b.logf("[replay] op=%s id=%s#%s status=recovered-existing new-offset=%s",
				opID, tp.String(), itoa64(item.OriginOffset), itoa64(newOff))
			op, _ = b.replays.getOp(opID)
			continue
		}

		// 预留新位移，先把账本落到“已预留”，再追加日志（崩溃窗口由上面的探测兜底）。
		newOff := l.endOffset()
		if newOff >= int64(l.capacity) {
			b.replays.markItemError(opID, item, ErrQueueFull.Error(), b.clock.Now())
			b.logf("[replay] op=%s id=%s#%s status=backpressure offset=%s reason=queue-full",
				opID, tp.String(), itoa64(item.OriginOffset), itoa64(newOff))
			return enqueuedNow, alreadyDone, ErrQueueFull
		}
		reserved := ReplayItem(item)
		reserved.NewOffset = newOff
		b.replays.markReserved(opID, reserved, now)

		dead, found := b.retries.deadEntry(op.Group, tp, item.OriginOffset)
		if !found {
			// 死信记录丢失（数据被外部清理）：不能凭空重投，报错保留现场。
			b.replays.markItemError(opID, item, "dead-letter record missing", b.clock.Now())
			return enqueuedNow, alreadyDone, ErrInvalidArgument
		}
		msg := buildReplayMessage(opID, dead, now)
		rec, err := l.Append(msg)
		if err != nil {
			b.replays.markItemError(opID, item, err.Error(), b.clock.Now())
			b.logf("[replay] op=%s id=%s#%s status=append-failed err=%v",
				opID, tp.String(), itoa64(item.OriginOffset), err)
			return enqueuedNow, alreadyDone, err
		}

		// 追加已 fsync：提交死信表与账本的“已入队”状态。
		b.retries.markReplayedEnqueued(op.Group, opID, tp, item.OriginOffset, rec.Offset, b.clock.Now())
		done := ReplayItem(item)
		done.Status = ReplayComplete
		done.NewOffset = rec.Offset
		b.replays.markEnqueued(opID, done, b.clock.Now())
		enqueuedNow++
		b.logf("[replay] op=%s id=%s#%s status=enqueued new-offset=%s",
			opID, tp.String(), itoa64(item.OriginOffset), itoa64(rec.Offset))

		// 重新读取操作快照（markEnqueued 不改变条目数量与顺序）。
		op, _ = b.replays.getOp(opID)
	}
	final, _ := b.replays.getOp(opID)
	b.logf("[replay] op=%s status=%s enqueued=%d already=%d items=%d",
		opID, string(final.Status),
		enqueuedNow, alreadyDone, len(final.Items))
	return enqueuedNow, alreadyDone, nil
}

// findReplayRecordLocked 在 [scanFrom, logEnd) 内查找属于本操作、源死信为
// (tp, originOffset) 的已写入重投记录。
func (b *Broker) findReplayRecordLocked(l *PartitionLog, opID string,
	tp TopicPartition, originOffset, scanFrom int64) (Record, int64, bool) {
	end := l.endOffset()
	for off := scanFrom; off < end; off++ {
		rec, ok := l.Read(off)
		if !ok {
			continue
		}
		prov, isReplay := ParseReplayProvenance(rec.Headers)
		if !isReplay || prov.OpID != opID {
			continue
		}
		if prov.OriginTopic == tp.Topic &&
			prov.OriginPartition == tp.Partition &&
			prov.OriginOffset == originOffset {
			return rec, off, true
		}
	}
	return Record{}, 0, false
}

// GetReplay 查询单个重投操作的当前状态（重启后亦可查询）。
// 查询在 Broker 互斥区内完成：与触发/续跑严格串行，返回的快照要么是
// 推进前的完整状态，要么是推进后的完整状态，绝不会读到半更新的中间态。
func (b *Broker) GetReplay(opID string) (ReplayOperation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if op, ok := b.replays.getOp(opID); ok {
		return op, nil
	}
	return ReplayOperation{}, ErrReplayNotFound
}

// ListReplays 列出某消费组的全部重投操作（含已完成与未完成），按创建时间排序。
// 与 GetReplay 同样在 Broker 互斥区内取快照，并发触发/续跑下安全一致。
func (b *Broker) ListReplays(groupName string) []ReplayOperation {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.replays.listOps(groupName)
}
