package mq

import "time"

// Message is a single queue record. Offset is assigned by the broker
// (starting at 0 per partition) and is zero on input.
type Message struct {
	Key     string
	Headers map[string]string
	Body    []byte
	Offset  int64
	Time    time.Time
}

// Delivered is a message handed to a consumer, carrying delivery metadata.
type Delivered struct {
	Topic     string
	Partition int
	Message   Message
	Delivery  int64 // delivery attempt: 1 = first delivery, 2+ = redelivery
}

// Backoff computes deterministic retry delays.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// ConstantBackoff always returns the same delay (deterministic).
type ConstantBackoff struct{ D time.Duration }

// Delay returns the constant delay. attempt (1-based negative-ack count) is
// ignored on purpose: the schedule is fully deterministic and reproducible.
func (b ConstantBackoff) Delay(attempt int) time.Duration {
	if b.D < 0 {
		return 0
	}
	return b.D
}

// LinearBackoff grows as Base * attempt (deterministic, no jitter).
type LinearBackoff struct{ Base time.Duration }

// Delay returns Base*attempt. Deliberately no random jitter so test runs
// and restarts reproduce the exact same redelivery schedule.
func (b LinearBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if b.Base < 0 {
		return 0
	}
	return b.Base * time.Duration(attempt)
}

// BrokerConfig configures a Broker.
type BrokerConfig struct {
	RootDir        string
	Partitions     int
	PartitionCap   int64 // max messages per partition; 0 = unbounded
	HighWatermark  int64 // soft capacity at which backpressure is reported; 0 = disabled
	MaxRetries     int   // negative nack attempts before dead-letter; 0 = dead-letter immediately
	LeaseTTL       time.Duration
	Backoff        Backoff
	Now            func() time.Time
	ProduceTimeout time.Duration
	FetchTimeout   time.Duration
}

// PartitionStats exposes observability for one partition.
type PartitionStats struct {
	Topic           string
	Partition       int
	LogStart        int64
	LogEnd          int64            // next offset to be written
	Lag             int64            // backlog vs slowest group
	Committed       map[string]int64 // group -> committed offset
	InFlight        int
	RetryPending    int
	DeadLettered    int
	Watermark       int64
	AtHighWatermark bool
	Full            bool
}

// GroupStats exposes consumer-group observability.
type GroupStats struct {
	Group       string
	Generation  int64
	Members     int
	Assignments map[string][]TopicPartition
}

// TopicPartition is a partition coordinate.
type TopicPartition struct {
	Topic     string
	Partition int
}
