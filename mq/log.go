package mq

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PartitionLog 是单个分区的顺序追加日志（磁盘文件 + 内存索引）。
// 同分区消息严格按追加顺序分配单调递增位移（从 0 开始）。
//
// 磁盘格式：每条记录 = 8 字节大端长度前缀 + 记录体；
// 每次 Append 后 fsync，进程崩溃只会截断最后一条半写记录，不会破坏顺序。
type PartitionLog struct {
	mu       sync.Mutex
	tp       TopicPartition
	file     *os.File
	recs     []Record
	capacity int
}

// LogStats 描述一个分区的可观测水位状态。
type LogStats struct {
	Topic          string
	Partition      int
	LogStartOffset int64
	LogEndOffset   int64 // 下一条将分配的位移
	Count          int64
	Capacity       int64
	Used           float64 // 容量使用率 0~1
	HighWatermark  bool    // 使用率达到 80%
	Full           bool    // 达到容量，Produce 将被拒绝
}

// highWatermarkRatio 触发高水位告警的容量比例。
const highWatermarkRatio = 0.8

func logFilePath(dir string, tp TopicPartition) string {
	return filepath.Join(dir, "logs", tp.Topic+"-"+itoa(tp.Partition)+".log")
}

func newPartitionLog(tp TopicPartition, dir string, capacity int) (*PartitionLog, error) {
	if capacity <= 0 {
		return nil, ErrInvalidArgument
	}
	path := logFilePath(dir, tp)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &PartitionLog{tp: tp, file: f, capacity: capacity}
	if err := l.recoverLocked(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// recoverLocked 从头扫描日志，重建内存索引；忽略尾部半写记录。
func (l *PartitionLog) recoverLocked() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(l.file)
	if err != nil {
		return err
	}
	var off int
	for off < len(data) {
		if len(data)-off < 8 {
			break // 半写长度前缀：截断
		}
		n := int(binary.BigEndian.Uint64(data[off : off+8]))
		off += 8
		if n <= 0 || off+n > len(data) {
			break // 半写记录体：截断
		}
		var rec Record
		if err := decodeRecord(data[off:off+n], &rec); err != nil {
			// 兼容旧版本日志（记录体不含主题/分区字段）：按旧布局再解一次，
			// 主题/分区用本日志已知的 l.tp 回填，保证升级后老数据可读、可 Ack。
			if err2 := decodeRecordLegacy(data[off:off+n], &rec); err2 != nil {
				break // 新旧布局都解不开：损坏记录，保守截断
			}
			rec.Topic = l.tp.Topic
			rec.Partition = l.tp.Partition
		}
		if rec.Offset != int64(len(l.recs)) {
			break // 位移不连续：保守截断
		}
		l.recs = append(l.recs, rec)
		off += n
	}
	if off < len(data) {
		// 截掉尾部无效字节，后续 Append 覆盖之。
		if err := l.file.Truncate(int64(off)); err != nil {
			return err
		}
	}
	_, err = l.file.Seek(0, io.SeekEnd)
	return err
}

// Append 顺序追加一条消息，返回分配到的记录。
func (l *PartitionLog) Append(m Message) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.recs) >= l.capacity {
		return Record{}, ErrQueueFull
	}
	rec := Record{
		Topic:      l.tp.Topic,
		Partition:  l.tp.Partition,
		Offset:     int64(len(l.recs)),
		Key:        append([]byte(nil), m.Key...),
		Value:      append([]byte(nil), m.Value...),
		Headers:    cloneHeaders(m.Headers),
		EnqueuedAt: time.Now(),
	}
	body := encodeRecord(rec)
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], uint64(len(body)))
	if _, err := l.file.Write(prefix[:]); err != nil {
		return Record{}, err
	}
	if _, err := l.file.Write(body); err != nil {
		return Record{}, err
	}
	if err := l.file.Sync(); err != nil {
		return Record{}, err
	}
	l.recs = append(l.recs, rec)
	return rec, nil
}

// Read 读取指定位移的记录。
func (l *PartitionLog) Read(offset int64) (Record, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < 0 || offset >= int64(len(l.recs)) {
		return Record{}, false
	}
	r := l.recs[int(offset)]
	r.Key = append([]byte(nil), r.Key...)
	r.Value = append([]byte(nil), r.Value...)
	return r, true
}

// endOffset 返回下一条将分配的位移（加锁由调用方或内部使用）。
func (l *PartitionLog) endOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(len(l.recs))
}

// Stats 返回分区水位与容量状态。
func (l *PartitionLog) Stats() LogStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := int64(len(l.recs))
	capN := int64(l.capacity)
	used := float64(n) / float64(capN)
	return LogStats{
		Topic:          l.tp.Topic,
		Partition:      l.tp.Partition,
		LogStartOffset: 0,
		LogEndOffset:   n,
		Count:          n,
		Capacity:       capN,
		Used:           used,
		HighWatermark:  used >= highWatermarkRatio,
		Full:           n >= capN,
	}
}

func (l *PartitionLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

func cloneHeaders(h map[string]string) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

func putBytes(b *bytes.Buffer, p []byte) {
	var l [8]byte
	binary.BigEndian.PutUint64(l[:], uint64(len(p)))
	b.Write(l[:])
	b.Write(p)
}

func putString(b *bytes.Buffer, s string) {
	var l [8]byte
	binary.BigEndian.PutUint64(l[:], uint64(len(s)))
	b.Write(l[:])
	b.WriteString(s)
}

func putInt64(b *bytes.Buffer, n int64) {
	var l [8]byte
	binary.BigEndian.PutUint64(l[:], uint64(n))
	b.Write(l[:])
}

func encodeRecord(r Record) []byte {
	var b bytes.Buffer
	putInt64(&b, r.Offset)
	putInt64(&b, r.EnqueuedAt.UnixNano())
	putString(&b, r.Topic)
	putInt64(&b, int64(r.Partition))
	putBytes(&b, r.Key)
	putBytes(&b, r.Value)
	putInt64(&b, int64(len(r.Headers)))
	for k, v := range r.Headers {
		putString(&b, k)
		putString(&b, v)
	}
	return b.Bytes()
}

func getBytes(data []byte, off *int) ([]byte, bool) {
	if *off+8 > len(data) {
		return nil, false
	}
	n := int(binary.BigEndian.Uint64(data[*off : *off+8]))
	*off += 8
	if n < 0 || *off+n > len(data) {
		return nil, false
	}
	out := append([]byte(nil), data[*off:*off+n]...)
	*off += n
	return out, true
}

func getString(data []byte, off *int) (string, bool) {
	p, ok := getBytes(data, off)
	return string(p), ok
}

func getInt64(data []byte, off *int) (int64, bool) {
	if *off+8 > len(data) {
		return 0, false
	}
	n := int64(binary.BigEndian.Uint64(data[*off : *off+8]))
	*off += 8
	return n, true
}

func decodeRecord(data []byte, r *Record) error {
	off := 0
	var ok bool
	if r.Offset, ok = getInt64(data, &off); !ok {
		return ErrInvalidArgument
	}
	tsNano, ok := getInt64(data, &off)
	if !ok {
		return ErrInvalidArgument
	}
	r.EnqueuedAt = time.Unix(0, tsNano)
	if topic, ok := getString(data, &off); !ok {
		return ErrInvalidArgument
	} else {
		r.Topic = topic
	}
	pn, ok := getInt64(data, &off)
	if !ok {
		return ErrInvalidArgument
	}
	r.Partition = int(pn)
	if r.Key, ok = getBytes(data, &off); !ok {
		return ErrInvalidArgument
	}
	if r.Value, ok = getBytes(data, &off); !ok {
		return ErrInvalidArgument
	}
	hN, ok := getInt64(data, &off)
	if !ok || hN < 0 {
		return ErrInvalidArgument
	}
	if hN > 0 {
		r.Headers = make(map[string]string, hN)
		for i := int64(0); i < hN; i++ {
			k, ok1 := getString(data, &off)
			v, ok2 := getString(data, &off)
			if !ok1 || !ok2 {
				return ErrInvalidArgument
			}
			r.Headers[k] = v
		}
	}
	// 必须恰好消费完整条记录体，否则可能是旧格式被误当新格式解出半截。
	if off != len(data) {
		return ErrInvalidArgument
	}
	return nil
}

// decodeRecordLegacy 解码旧版本记录布局：offset、enqueuedAt 之后直接是
// Key/Value/Headers（不含主题与分区）；主题/分区由 recoverLocked 回填。
func decodeRecordLegacy(data []byte, r *Record) error {
	off := 0
	var ok bool
	if r.Offset, ok = getInt64(data, &off); !ok {
		return ErrInvalidArgument
	}
	tsNano, ok := getInt64(data, &off)
	if !ok {
		return ErrInvalidArgument
	}
	r.EnqueuedAt = time.Unix(0, tsNano)
	if r.Key, ok = getBytes(data, &off); !ok {
		return ErrInvalidArgument
	}
	if r.Value, ok = getBytes(data, &off); !ok {
		return ErrInvalidArgument
	}
	hN, ok := getInt64(data, &off)
	if !ok || hN < 0 {
		return ErrInvalidArgument
	}
	if hN > 0 {
		r.Headers = make(map[string]string, hN)
		for i := int64(0); i < hN; i++ {
			k, ok1 := getString(data, &off)
			v, ok2 := getString(data, &off)
			if !ok1 || !ok2 {
				return ErrInvalidArgument
			}
			r.Headers[k] = v
		}
	}
	if off != len(data) {
		return ErrInvalidArgument
	}
	return nil
}
