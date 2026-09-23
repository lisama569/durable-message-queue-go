package mq

import "time"

// partition owns one ordered append log for a topic partition.
type partition struct {
	topic     string
	id        int
	log       *recordLog
	capacity  int64 // hard cap on log size (end - base); 0 = unbounded
	watermark int64 // soft cap reported via stats; 0 = disabled
}

func newPartition(dir, topic string, id int, capacity, watermark int64) (*partition, error) {
	l, err := openRecordLog(dir, topic, id)
	if err != nil {
		return nil, err
	}
	return &partition{
		topic:     topic,
		id:        id,
		log:       l,
		capacity:  capacity,
		watermark: watermark,
	}, nil
}

// lag is the backlog depth relative to the slowest consumer: logEnd minus
// the smallest committed offset across all groups. Capacity and the high
// watermark are measured against this lag, so acknowledging messages frees
// capacity under backpressure.
func (p *partition) lag(minCommitted int64) int64 {
	lag := p.log.end() - minCommitted
	if lag < 0 {
		return 0
	}
	return lag
}

// atHighWatermark reports whether the slowest-group lag reached the soft mark.
func (p *partition) atHighWatermark(lag int64) bool {
	return p.watermark > 0 && lag >= p.watermark
}

// full reports whether the slowest-group lag reached hard capacity.
func (p *partition) full(lag int64) bool {
	return p.capacity > 0 && lag >= p.capacity
}

// append stores msg, assigns its offset. Capacity enforcement is done by
// the broker, which knows every group's committed offset.
func (p *partition) append(msg Message, now time.Time) (int64, error) {
	if msg.Time.IsZero() {
		msg.Time = now
	}
	return p.log.append(record{
		Offset:  -1, // assigned by the log
		TimeNS:  msg.Time.UnixNano(),
		Key:     msg.Key,
		Headers: msg.Headers,
		Body:    msg.Body,
	})
}

func (p *partition) read(offset int64) (record, error) { return p.log.read(offset) }

func (p *partition) logEnd() int64 { return p.log.end() }

func (p *partition) logStart() int64 { return p.log.baseOff }

func (p *partition) close() error { return p.log.close() }
