package mq

import (
	"time"
)

// ReplayDeadLetters 把某消费组中符合筛选条件的死信作为“新记录”追加回对应分区。
//
// 语义：
//   - 重投消息以全新位移追加到同分区日志尾部，顺序由日志保证：
//     不回退已提交位移、不重投已确认消息，也不会跑到同分区现有未决消息“前面”，
//     但会按分区追加顺序排在其后（同分区同时刻仍只有一个持有者处理）；
//   - 新记录携带保留消息头（重投请求 ID、原始位移、原始失败次数与原因链、
//     原始生产时间、重投跳数），重新获得完整的重试预算；再次超过上限时
//     仍按确定性退避进死信，且新死信的 Origin 指向其上一跳；
//   - 容量反压：追加前逐分区做容量预检，任一分区装不下整批则整批拒绝
//     （ErrQueueFull），不部分入队；在途反压与正常消息共用 MaxInflight；
//   - 幂等与崩溃安全：请求先落盘再追加；已被其它待完成请求认领或已重投的
//     死信不会重复选中；崩溃重启后 ResumePendingReplays 依据日志中的
//     重投头对账，已 fsync 的条目不会重复追加，未完成条目继续追加。
func (b *Broker) ReplayDeadLetters(groupName string, filter ReplayFilter) (ReplayResult, error) {
	if groupName == "" {
		return ReplayResult{}, ErrInvalidArgument
	}
	if !filter.DeadAtFrom.IsZero() && !filter.DeadAtTo.IsZero() &&
		filter.DeadAtTo.Before(filter.DeadAtFrom) {
		return ReplayResult{}, ErrInvalidArgument
	}
	now := b.clock.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	// 先把历史遗留的待完成请求续跑完，避免同一条死信被重复认领。
	if err := b.resumePendingLocked(now); err != nil {
		return ReplayResult{}, err
	}

	// 待完成请求认领过的死信视为不可重复选中。
	skip := b.claimedDeadLocked(groupName)

	picked := b.retries.selectDead(groupName, filter, skip)
	if len(picked) == 0 {
		b.logf("[replay] group=%s status=nothing-matched filter=%+v", groupName, filter)
		return ReplayResult{}, ErrNothingToReplay
	}

	// 组装请求条目（稳定顺序：主题、分区、位移），并记录各分区追加起点。
	items := make([]replayItemState, 0, len(picked))
	byTP := map[TopicPartition][]int{}
	capNeed := map[TopicPartition]int{}
	for _, d := range picked {
		tp := d.Record.topicPartition()
		if _, ok := b.logs[tp]; !ok {
			// 主题日志不存在（数据目录被破坏）：明确报错而非静默跳过。
			return ReplayResult{}, ErrPartitionNotFound
		}
		capNeed[tp]++
		byTP[tp] = append(byTP[tp], len(items))
		items = append(items, replayItemState{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			OldOffset: d.Offset,
			NewOffset: -1,
		})
	}
	// 容量预检：每个分区一次性装入本请求分配到的全部条目。
	for tp, need := range capNeed {
		l := b.logs[tp]
		if l.len()+int64(need) > int64(b.cfg.PartCapacity) {
			b.logf("[replay] group=%s tp=%s status=rejected reason=capacity need=%d len=%d cap=%d",
				groupName, tp.String(), need, l.len(), b.cfg.PartCapacity)
			return ReplayResult{}, ErrQueueFull
		}
	}
	for tp, idxs := range byTP {
		base := b.logs[tp].len()
		for _, i := range idxs {
			items[i].Base = base
		}
	}

	reqID := b.replays.nextID(now)
	req := replayRequest{
		RequestID: reqID,
		Group:     groupName,
		CreatedAt: now,
		Status:    ReplayPending,
		Items:     items,
	}
	// 1) 先持久化请求：保证崩溃后可恢复、不重复不漏。
	if err := b.replays.create(req); err != nil {
		return ReplayResult{}, err
	}
	b.logf("[replay] group=%s request=%s status=created items=%d",
		groupName, reqID, len(items))

	// 2) 执行追加（内部负责崩溃对账与死信认领）。
	req, err := b.runReplayLocked(req, now)
	if err != nil {
		return ReplayResult{}, err
	}
	return toReplayResult(req), nil
}

// claimedDeadLocked 汇总该组所有 pending 请求已认领的死信键。
func (b *Broker) claimedDeadLocked(groupName string) map[deadKey]bool {
	skip := map[deadKey]bool{}
	for _, r := range b.replays.list(groupName) {
		if r.Status != ReplayPending {
			continue
		}
		for _, it := range r.Items {
			skip[deadKey{tp: it.tp(), offset: it.OldOffset}] = true
		}
	}
	return skip
}

// runReplayLocked 执行（或恢复执行）一个重投请求：
//  1. 对每个分区，从 Base 起扫描日志，按“重投请求 ID + 原始位移”对账，
//     标出崩溃前已经 fsync 落盘的条目（绝不重复追加）；
//  2. 把仍缺失的条目按分区整批原子追加（AppendMany 容量复核）；
//  3. 每完成一个分区即持久化进度并把对应死信标记为已重投；
//  4. 全部完成后请求置为 completed。
//
// 中途遇到 ErrQueueFull（并发 Produce 占满容量）则保留 pending 状态并返回，
// 后续重试/重启恢复时继续——不丢、不重。
// 调用方必须持有 b.mu。
func (b *Broker) runReplayLocked(req replayRequest, now time.Time) (replayRequest, error) {
	// 按分区聚合并保持分区处理顺序稳定。
	byTP := map[TopicPartition][]int{}
	var tps []TopicPartition
	for i := range req.Items {
		tp := req.Items[i].tp()
		if _, ok := byTP[tp]; !ok {
			tps = append(tps, tp)
		}
		byTP[tp] = append(byTP[tp], i)
	}
	sortTP(tps)

	for _, tp := range tps {
		idxs := byTP[tp]
		l := b.logs[tp]

		// 1) 对账：从 Base 起扫描，找出本请求已落盘的条目。
		existing := map[int64]int64{} // oldOffset -> newOffset
		end := l.endOffset()
		for off := req.Items[idxs[0]].Base; off < end; off++ {
			rec, ok := l.Read(off)
			if !ok {
				break
			}
			o := originFromHeaders(rec.Headers)
			if o != nil && o.RequestID == req.RequestID {
				existing[o.Offset] = off
			}
		}

		// 组装本分区仍缺失的条目（保持 idxs 即旧位移升序，顺序确定）。
		type todo struct {
			idx  int
			dead DLQEntry
		}
		var pending []todo
		var doneKeys []deadKey
		for _, i := range idxs {
			it := &req.Items[i]
			if newOff, ok := existing[it.OldOffset]; ok {
				// 崩溃前已落盘：只补齐内存进度，绝不重复追加。
				if !it.Appended {
					it.Appended = true
					it.NewOffset = newOff
					doneKeys = append(doneKeys, deadKey{tp: tp, offset: it.OldOffset})
				}
				continue
			}
			if it.Appended {
				// 内存标记已追加却扫描不到：只可能是日志被外部截断，明确报错。
				return req, ErrOffsetOutOfRange
			}
			d, ok := b.retries.findDead(req.Group, deadKey{tp: tp, offset: it.OldOffset})
			if !ok {
				return req, ErrInvalidArgument
			}
			pending = append(pending, todo{idx: i, dead: d})
		}

		// 2) 缺失条目整批原子追加（容量在这里复核反压）。
		if len(pending) > 0 {
			msgs := make([]Message, 0, len(pending))
			for _, t := range pending {
				msgs = append(msgs, b.buildReplayMessageLocked(req, t.dead))
			}
			recs, err := l.AppendMany(msgs)
			if err != nil {
				b.logf("[replay] request=%s tp=%s status=pending reason=%v remaining=%d",
					req.RequestID, tp.String(), err, len(pending))
				return req, err
			}
			for j, t := range pending {
				it := &req.Items[t.idx]
				it.Appended = true
				it.NewOffset = recs[j].Offset
				doneKeys = append(doneKeys, deadKey{tp: tp, offset: it.OldOffset})
			}
		}

		// 3) 该分区全部条目已落盘：先认领死信，再持久化本分区进度。
		if len(doneKeys) > 0 {
			if err := b.retries.markReplayed(req.Group, req.RequestID, doneKeys); err != nil {
				return req, err
			}
		}
		if err := b.replays.update(req); err != nil {
			return req, err
		}
		b.logf("[replay] request=%s tp=%s status=partition-done count=%d",
			req.RequestID, tp.String(), len(idxs))
	}

	// 4) 全部完成。
	req.Status = ReplayCompleted
	if err := b.replays.update(req); err != nil {
		return req, err
	}
	b.logf("[replay] request=%s group=%s status=completed items=%d",
		req.RequestID, req.Group, len(req.Items))
	return req, nil
}

// buildReplayMessageLocked 以死信快照构造重投新消息：复制 Key/Value 与用户头，
// 附加保留来源头。若是“再次重投”，跳数与原始生产时间沿链继承，失败原因链
// 使用上一跳死信中已经累积的完整链。
func (b *Broker) buildReplayMessageLocked(req replayRequest, d DLQEntry) Message {
	hop := 1
	firstEnqueued := d.Record.EnqueuedAt
	reasons := append([]string(nil), d.Reasons...)
	attempts := d.Attempts
	originOffset := d.Offset
	originTP := d.Record.topicPartition()
	// 上一跳本身就是重投消息：沿用其原始来源与跳数。
	if prev := d.Origin; prev != nil {
		hop = prev.Hop + 1
		firstEnqueued = prev.FirstEnqueuedAt
		originOffset = prev.Offset
		originTP = prev.tp()
		attempts = prev.Attempts
	}
	o := ReplayOrigin{
		RequestID:       req.RequestID,
		Group:           req.Group,
		Topic:           originTP.Topic,
		Partition:       originTP.Partition,
		Offset:          originOffset,
		Attempts:        attempts,
		Reasons:         reasons,
		DeadAt:          d.DeadAt,
		FirstEnqueuedAt: firstEnqueued,
		Hop:             hop,
	}
	return Message{
		Key:     append([]byte(nil), d.Record.Key...),
		Value:   append([]byte(nil), d.Record.Value...),
		Headers: encodeOriginHeaders(d.Record.Headers, o),
	}
}

// resumePendingLocked 续跑全部组中尚未完成的重投请求。
// 调用方必须持有 b.mu。
func (b *Broker) resumePendingLocked(now time.Time) error {
	for _, req := range b.replays.pending() {
		// 若对应日志尚未打开（例如重启后未调用 CreateTopic），跳过等下一拍。
		missing := false
		for _, it := range req.Items {
			if _, ok := b.logs[it.tp()]; !ok {
				missing = true
				break
			}
		}
		if missing {
			continue
		}
		updated, err := b.runReplayLocked(req, now)
		if err != nil {
			if err == ErrQueueFull {
				// 反压属正常待续跑情形：保留 pending，交由后续调用重试。
				b.logf("[replay-resume] request=%s status=backpressure", req.RequestID)
				continue
			}
			return err
		}
		req = updated
	}
	return nil
}

// ReplayStatus 查询某次重投请求的状态与逐条结果。
func (b *Broker) ReplayStatus(groupName, requestID string) (ReplayResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	req, ok := b.replays.get(groupName, requestID)
	if !ok {
		return ReplayResult{}, ErrReplayNotFound
	}
	return toReplayResult(req), nil
}

// ListReplays 列出某消费组登记过的重投请求（含已完成与待续跑），按登记顺序。
func (b *Broker) ListReplays(groupName string) []ReplayResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	reqs := b.replays.list(groupName)
	out := make([]ReplayResult, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, toReplayResult(req))
	}
	return out
}

// ResumePendingReplays 主动续跑崩溃后尚未完成的重投请求；通常无需手动调用，
// Poll/ReplayDeadLetters 会自动触发。遇到容量反压时保留 pending 并继续处理其余请求。
func (b *Broker) ResumePendingReplays() error {
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.resumePendingLocked(now)
}

func sortTP(tps []TopicPartition) {
	for i := 1; i < len(tps); i++ {
		for j := i; j > 0; j-- {
			a, z := tps[j-1], tps[j]
			less := a.Topic < z.Topic || (a.Topic == z.Topic && a.Partition < z.Partition)
			if !less {
				tps[j-1], tps[j] = z, a
			}
		}
	}
}

// toReplayResult 把持久化模型转为对外视图。
func toReplayResult(r replayRequest) ReplayResult {
	out := ReplayResult{
		RequestID: r.RequestID,
		Group:     r.Group,
		Status:    r.Status,
		CreatedAt: r.CreatedAt,
		Items:     make([]ReplayItem, 0, len(r.Items)),
	}
	for _, it := range r.Items {
		out.Items = append(out.Items, ReplayItem{
			Topic:     it.Topic,
			Partition: it.Partition,
			OldOffset: it.OldOffset,
			NewOffset: it.NewOffset,
		})
	}
	return out
}
