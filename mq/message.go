package mq

import "time"

// TopicPartition 唯一标识一个主题分区。
type TopicPartition struct {
	Topic     string
	Partition int
}

func (tp TopicPartition) String() string {
	return tp.Topic + "-" + itoa(tp.Partition)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// MessageID 返回可在日志中稳定识别一条消息的字符串：topic-partition#offset。
func (r Record) MessageID() string {
	return r.Topic + "-" + itoa(r.Partition) + "#" + itoa64(r.Offset)
}

func (r Record) topicPartition() TopicPartition {
	return TopicPartition{Topic: r.Topic, Partition: r.Partition}
}

func (d Delivery) String() string {
	return "delivery{" + d.Record.MessageID() +
		" group=" + d.Group +
		" consumer=" + d.ConsumerID +
		" attempt=" + itoa(d.Attempt) +
		" seq=" + itoa64(d.DeliverySeq) + "}"
}

// Message 是用户投递的消息体。
type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Record 是消息落到分区日志后带位移的投递单元。
type Record struct {
	Topic      string
	Partition  int
	Offset     int64
	Key        []byte
	Value      []byte
	Headers    map[string]string
	EnqueuedAt time.Time
}

// Delivery 是投递给消费者的一条记录，携带投递元数据。
type Delivery struct {
	Record      Record
	Group       string
	ConsumerID  string
	DeliverySeq int64
	Attempt     int
	DeliveredAt time.Time
}

// Sentinel errors —— 调用方可用 errors.Is 明确区分失败原因。
var (
	ErrPartitionNotFound  = mqError("partition not found")
	ErrPartitionOwned     = mqError("partition already owned by another consumer")
	ErrNotOwner           = mqError("consumer is not the current owner of the partition")
	ErrLeaseExpired       = mqError("consumer lease expired; partition ownership lost")
	ErrStaleGeneration    = mqError("stale consumer generation; rebalance has occurred")
	ErrOffsetRolledBack   = mqError("offset commit rejected: smaller than committed offset")
	ErrOffsetOutOfRange   = mqError("offset commit rejected: offset beyond log end")
	ErrQueueFull          = mqError("partition queue capacity reached (high watermark)")
	ErrRetryExhausted     = mqError("retry attempts exhausted; message moved to dead-letter")
	ErrInvalidArgument    = mqError("invalid argument")
	ErrConsumerRegistered = mqError("consumer already registered in group")
	ErrUnknownConsumer    = mqError("unknown consumer")
	ErrNothingToReplay    = mqError("no eligible dead letters matched the replay filter")
	ErrReplayNotFound     = mqError("replay request not found")
)

type mqError string

func (e mqError) Error() string { return string(e) }
