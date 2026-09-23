package mq

import (
	"fmt"
	"sort"
	"time"
)

// tpSep is used to serialize TopicPartition as a JSON map key.
const tpSep = "#"

func (tp TopicPartition) String() string { return tp.Topic + tpSep + itoa(tp.Partition) }

func parseTP(s string) (TopicPartition, error) {
	for i := 0; i < len(s); i++ {
		if s[i] == tpSep[0] {
			p, err := atoi(s[i+1:])
			if err != nil {
				return TopicPartition{}, fmt.Errorf("mq: bad topic-partition %q", s)
			}
			return TopicPartition{Topic: s[:i], Partition: p}, nil
		}
	}
	return TopicPartition{}, fmt.Errorf("mq: bad topic-partition %q", s)
}

// member is a consumer in a group.
type member struct {
	id       string
	lastBeat time.Time
}

// group is a consumer group with generations, leases, and assignments.
type group struct {
	name       string
	generation int64
	members    map[string]*member
	universe   []TopicPartition
	assignment map[TopicPartition]string
}

// coordinator manages all groups and their persisted state.
type coordinator struct {
	path     string
	lease    time.Duration
	groups   map[string]*group
	now      func() time.Time
	onRevoke func(groupID string, revoked []TopicPartition, gen int64)
}

type groupState struct {
	Generation int64             `json:"generation"`
	Members    map[string]int64  `json:"members"`    // id -> last heartbeat unix nano
	Assignment map[string]string `json:"assignment"` // "topic#partition" -> member id
	Universe   []string          `json:"universe"`   // ordered partition coordinates
}

func openCoordinator(path string, lease time.Duration, now func() time.Time) (*coordinator, error) {
	c := &coordinator{
		path:   path,
		lease:  lease,
		groups: make(map[string]*group),
		now:    now,
	}
	var raw map[string]groupState
	if err := readJSON(path, &raw); err != nil {
		return nil, fmt.Errorf("mq: load groups: %w", err)
	}
	for name, st := range raw {
		g := &group{
			name:       name,
			generation: st.Generation,
			members:    make(map[string]*member),
			assignment: make(map[TopicPartition]string),
		}
		for id, ns := range st.Members {
			g.members[id] = &member{id: id, lastBeat: time.Unix(0, ns)}
		}
		for tpStr, m := range st.Assignment {
			tp, err := parseTP(tpStr)
			if err != nil {
				return nil, err
			}
			g.assignment[tp] = m
		}
		for _, tpStr := range st.Universe {
			tp, err := parseTP(tpStr)
			if err != nil {
				return nil, err
			}
			g.universe = append(g.universe, tp)
		}
		c.groups[name] = g
	}
	return c, nil
}

func (c *coordinator) setRevokeHook(fn func(string, []TopicPartition, int64)) {
	c.onRevoke = fn
}

// setUniverse replaces the known set of partitions for every group.
func (c *coordinator) setUniverse(parts []TopicPartition) error {
	sorted := append([]TopicPartition(nil), parts...)
	sortTPs(sorted)
	for _, g := range c.groups {
		if !sameUniverse(g.universe, sorted) {
			g.universe = append([]TopicPartition(nil), sorted...)
			c.rebalanceLocked(g)
		}
	}
	return c.persistLocked()
}

// Join adds a member and triggers deterministic round-robin rebalance.
// universe is the broker's current full partition set; it is adopted by
// the group before assignment so newly created topics are included.
func (c *coordinator) join(groupID, memberID string, universe []TopicPartition) (int64, []TopicPartition, error) {
	g, ok := c.groups[groupID]
	if !ok {
		g = &group{
			name:       groupID,
			members:    make(map[string]*member),
			assignment: make(map[TopicPartition]string),
		}
		c.groups[groupID] = g
	}
	sorted := append([]TopicPartition(nil), universe...)
	sortTPs(sorted)
	if !sameUniverse(g.universe, sorted) {
		g.universe = sorted
	}
	// Register/refresh the joining member *before* reaping so a join that
	// happens after the TTL window does not evict the joiner itself.
	if _, exists := g.members[memberID]; !exists {
		g.members[memberID] = &member{id: memberID}
	}
	g.members[memberID].lastBeat = c.now()
	// Reap only *other* expired members.
	c.expireExceptLocked(g, memberID)
	c.rebalanceLocked(g)
	if err := c.persistLocked(); err != nil {
		return 0, nil, err
	}
	return g.generation, c.memberAssignments(g, memberID), nil
}

// expireExceptLocked removes members past their lease, optionally skipping
// one member (the member currently renewing its own lease).
func (c *coordinator) expireExceptLocked(g *group, skip string) []TopicPartition {
	var dead []string
	now := c.now()
	for id, m := range g.members {
		if id == skip {
			continue
		}
		if now.Sub(m.lastBeat) > c.lease {
			dead = append(dead, id)
		}
	}
	if len(dead) == 0 {
		return nil
	}
	for _, id := range dead {
		delete(g.members, id)
	}
	return c.rebalanceLocked(g)
}

func (c *coordinator) heartbeat(groupID, memberID string) (int64, error) {
	g, ok := c.groups[groupID]
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrUnknownGroup, groupID)
	}
	m, ok := g.members[memberID]
	if !ok {
		return g.generation, fmt.Errorf("%w: %s", ErrUnknownMember, memberID)
	}
	// Renew self first, then reap other expired members.
	m.lastBeat = c.now()
	c.expireExceptLocked(g, memberID)
	if err := c.persistLocked(); err != nil {
		return 0, err
	}
	return g.generation, nil
}

func (c *coordinator) leave(groupID, memberID string) error {
	g, ok := c.groups[groupID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownGroup, groupID)
	}
	if _, ok := g.members[memberID]; !ok {
		return fmt.Errorf("%w: %s", ErrUnknownMember, memberID)
	}
	delete(g.members, memberID)
	c.rebalanceLocked(g)
	return c.persistLocked()
}

// checkOwner verifies gen is current, the member is alive, and owns tp.
func (c *coordinator) checkOwner(groupID, memberID string, tp TopicPartition, gen int64) error {
	g, ok := c.groups[groupID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownGroup, groupID)
	}
	m, ok := g.members[memberID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownMember, memberID)
	}
	if g.generation != gen {
		return fmt.Errorf("%w: group=%s member=%s gen=%d current=%d",
			ErrStaleGeneration, groupID, memberID, gen, g.generation)
	}
	if c.now().Sub(m.lastBeat) > c.lease {
		return fmt.Errorf("%w: member=%s", ErrLeaseExpired, memberID)
	}
	if owner, ok := g.assignment[tp]; !ok || owner != memberID {
		return fmt.Errorf("%w: group=%s tp=%s member=%s owner=%q",
			ErrPartitionNotOwned, groupID, tp, memberID, owner)
	}
	return nil
}

func (c *coordinator) assignment(groupID string) (map[TopicPartition]string, int64, error) {
	g, ok := c.groups[groupID]
	if !ok {
		return nil, 0, fmt.Errorf("%w: %s", ErrUnknownGroup, groupID)
	}
	out := make(map[TopicPartition]string, len(g.assignment))
	for k, v := range g.assignment {
		out[k] = v
	}
	return out, g.generation, nil
}

func (c *coordinator) stats() map[string]GroupStats {
	out := make(map[string]GroupStats, len(c.groups))
	for id, g := range c.groups {
		amap := make(map[string][]TopicPartition)
		for tp, m := range g.assignment {
			amap[m] = append(amap[m], tp)
		}
		for m := range amap {
			sortTPs(amap[m])
		}
		out[id] = GroupStats{
			Group:       id,
			Generation:  g.generation,
			Members:     len(g.members),
			Assignments: amap,
		}
	}
	return out
}

// expire removes members whose lease elapsed (if any) and reassigns.
func (c *coordinator) expire(groupID string) {
	g, ok := c.groups[groupID]
	if !ok {
		return
	}
	c.expireLocked(g)
}

func (c *coordinator) expireLocked(g *group) []TopicPartition {
	return c.expireExceptLocked(g, "")
}

// rebalanceLocked assigns the (sorted) universe round-robin to (sorted)
// live members. Any change bumps the generation and revokes partitions
// whose owner changed so in-flight state can be invalidated.
// Caller must not hold any broker lock across onRevoke beyond what the
// hook itself requires.
func (c *coordinator) rebalanceLocked(g *group) []TopicPartition {
	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)
	parts := append([]TopicPartition(nil), g.universe...)
	sortTPs(parts)

	next := make(map[TopicPartition]string, len(parts))
	if len(memberIDs) > 0 {
		for i, tp := range parts {
			next[tp] = memberIDs[i%len(memberIDs)]
		}
	}
	var revoked []TopicPartition
	for tp, newOwner := range next {
		if old, ok := g.assignment[tp]; !ok || old != newOwner {
			revoked = append(revoked, tp)
		}
	}
	for tp, old := range g.assignment {
		if _, ok := next[tp]; !ok {
			_ = old
			revoked = append(revoked, tp)
		}
	}
	if len(revoked) == 0 && len(next) == len(g.assignment) {
		g.assignment = next
		return nil
	}
	g.assignment = next
	g.generation++
	sortTPs(revoked)
	if c.onRevoke != nil && len(revoked) > 0 {
		c.onRevoke(g.name, revoked, g.generation)
	}
	return revoked
}

func (c *coordinator) memberAssignments(g *group, memberID string) []TopicPartition {
	var out []TopicPartition
	for tp, owner := range g.assignment {
		if owner == memberID {
			out = append(out, tp)
		}
	}
	sortTPs(out)
	return out
}

func (c *coordinator) persistLocked() error {
	raw := make(map[string]groupState, len(c.groups))
	for name, g := range c.groups {
		st := groupState{
			Generation: g.generation,
			Members:    make(map[string]int64, len(g.members)),
			Assignment: make(map[string]string, len(g.assignment)),
			Universe:   make([]string, 0, len(g.universe)),
		}
		for id, m := range g.members {
			st.Members[id] = m.lastBeat.UnixNano()
		}
		for tp, owner := range g.assignment {
			st.Assignment[tp.String()] = owner
		}
		for _, tp := range g.universe {
			st.Universe = append(st.Universe, tp.String())
		}
		raw[name] = st
	}
	if err := writeJSONAtomically(c.path, raw); err != nil {
		return fmt.Errorf("mq: persist groups: %w", err)
	}
	return nil
}

func sortTPs(tps []TopicPartition) {
	sort.Slice(tps, func(i, j int) bool {
		if tps[i].Topic != tps[j].Topic {
			return tps[i].Topic < tps[j].Topic
		}
		return tps[i].Partition < tps[j].Partition
	})
}

func sameUniverse(a, b []TopicPartition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }

func atoi(s string) (int, error) {
	var n int
	neg := false
	if len(s) == 0 {
		return 0, fmt.Errorf("empty")
	}
	for i, ch := range s {
		if i == 0 && ch == '-' {
			neg = true
			continue
		}
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("bad int %q", s)
		}
		n = n*10 + int(ch-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}
