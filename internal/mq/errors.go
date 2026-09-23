package mq

import "fmt"

// Sentinel errors.
var (
	ErrClosed            = fmt.Errorf("mq: broker closed")
	ErrTopicExists       = fmt.Errorf("mq: topic already exists")
	ErrUnknownTopic      = fmt.Errorf("mq: unknown topic")
	ErrUnknownPartition  = fmt.Errorf("mq: unknown partition")
	ErrUnknownGroup      = fmt.Errorf("mq: unknown consumer group")
	ErrUnknownMember     = fmt.Errorf("mq: unknown group member")
	ErrPartitionNotOwned = fmt.Errorf("mq: partition not owned by member")
	ErrPartitionOwned    = fmt.Errorf("mq: partition already owned by another member")
	ErrStaleGeneration   = fmt.Errorf("mq: stale group generation")
	ErrLeaseExpired      = fmt.Errorf("mq: consumer lease expired")
	ErrPartitionFull     = fmt.Errorf("mq: partition at capacity (high watermark)")
	ErrNoMessage         = fmt.Errorf("mq: no message available")
	ErrRetryBackoff      = fmt.Errorf("mq: message in retry backoff")
	ErrDeadLettered      = fmt.Errorf("mq: message already moved to dead letter")
)

// OffsetError describes a rejected offset commit with a distinguishable reason.
type OffsetError struct {
	Topic     string
	Partition int
	Offset    int64
	Reason    OffsetRejectReason
	Detail    string
}

// OffsetRejectReason enumerates offset commit rejection reasons.
type OffsetRejectReason string

const (
	OffsetRejectRegress      OffsetRejectReason = "offset_regress"
	OffsetRejectOutOfRange   OffsetRejectReason = "offset_out_of_range"
	OffsetRejectNotCommitted OffsetRejectReason = "offset_predecessor_not_committed"
)

func (e *OffsetError) Error() string {
	return fmt.Sprintf("mq: offset commit rejected: topic=%s partition=%d offset=%d reason=%s %s",
		e.Topic, e.Partition, e.Offset, e.Reason, e.Detail)
}

func (e *OffsetError) Unwrap() error { return nil }
