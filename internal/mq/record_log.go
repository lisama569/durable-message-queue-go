package mq

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	frameHeaderLen = 8 + 4 // uint64 frameLen + uint32 crc32
	maxFrameLen    = 256 * 1024 * 1024
)

// record is the on-disk encoding of a Message.
type record struct {
	Offset  int64
	TimeNS  int64
	Key     string
	Headers map[string]string
	Body    []byte
}

// recordLog is an append-only, CRC-protected binary log:
//
//	[8 frameLen][4 crc32][payload]
//	payload = [8 offset][8 timeNS][2 keyLen][key]
//	          [2 hdrCount]([2 kLen]k[2 vLen]v)*[4 bodyLen]body
type recordLog struct {
	mu        sync.Mutex
	f         *os.File
	dir       string
	topic     string
	partition int
	baseOff   int64
	endOff    int64
	pos       map[int64]int64 // offset -> byte position of frameLen
}

func logPath(dir, topic string, partition int) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%d.log", topic, partition))
}

func openRecordLog(dir, topic string, partition int) (*recordLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := logPath(dir, topic, partition)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	l := &recordLog{
		f:         f,
		dir:       dir,
		topic:     topic,
		partition: partition,
		pos:       make(map[int64]int64),
	}
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// recover scans the whole log, indexes valid frames, and truncates any
// torn/corrupt tail so the file always ends on a complete valid record.
func (l *recordLog) recover() error {
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var (
		filePos    int64
		headBuf    [frameHeaderLen]byte
		first      = true
		expectOff  int64
		validUntil int64
	)
	for {
		n, err := io.ReadFull(l.f, headBuf[:])
		if err == io.EOF && n == 0 {
			break // clean end
		}
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			break // torn frame header
		}
		if err != nil {
			return err
		}
		frameLen := int64(binary.LittleEndian.Uint64(headBuf[0:8]))
		if frameLen < 0 || frameLen > maxFrameLen {
			break // implausible frame
		}
		payload := make([]byte, frameLen)
		if _, err := io.ReadFull(l.f, payload); err != nil {
			break // torn frame body
		}
		// CRC lives in the fixed header, not in the length-prefixed body.
		wantCRC := binary.LittleEndian.Uint32(headBuf[8:12])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			break // corrupt payload
		}
		rec, err := decodePayload(payload)
		if err != nil {
			break
		}
		if first {
			l.baseOff = rec.Offset
			expectOff = rec.Offset
			first = false
		}
		if rec.Offset != expectOff {
			break // gap or duplicate -> stop before this frame
		}
		l.pos[rec.Offset] = filePos
		expectOff++
		filePos += int64(frameHeaderLen) + frameLen
		validUntil = filePos
	}
	l.endOff = l.baseOff + int64(len(l.pos))
	// Truncate any torn/corrupt tail.
	fi, err := l.f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() != validUntil {
		if err := l.f.Truncate(validUntil); err != nil {
			return err
		}
		if _, err := l.f.Seek(0, io.SeekEnd); err != nil {
			return err
		}
	}
	// baseOff remains whatever the first persisted record declared (0 here).
	return nil
}

func encodePayload(rec record) []byte {
	var b []byte
	put := func(p []byte) { b = append(b, p...) }
	var u64 [8]byte
	var u32 [4]byte
	var u16 [2]byte
	binary.LittleEndian.PutUint64(u64[:], uint64(rec.Offset))
	put(u64[:])
	binary.LittleEndian.PutUint64(u64[:], uint64(rec.TimeNS))
	put(u64[:])
	binary.LittleEndian.PutUint16(u16[:], uint16(len(rec.Key)))
	put(u16[:])
	put([]byte(rec.Key))
	keys := make([]string, 0, len(rec.Headers))
	for k := range rec.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic encoding
	binary.LittleEndian.PutUint16(u16[:], uint16(len(keys)))
	put(u16[:])
	for _, k := range keys {
		v := rec.Headers[k]
		binary.LittleEndian.PutUint16(u16[:], uint16(len(k)))
		put(u16[:])
		put([]byte(k))
		binary.LittleEndian.PutUint16(u16[:], uint16(len(v)))
		put(u16[:])
		put([]byte(v))
	}
	binary.LittleEndian.PutUint32(u32[:], uint32(len(rec.Body)))
	put(u32[:])
	put(rec.Body)
	return b
}

func decodePayload(p []byte) (record, error) {
	var rec record
	if len(p) < 16 {
		return rec, io.ErrUnexpectedEOF
	}
	rec.Offset = int64(binary.LittleEndian.Uint64(p[0:8]))
	rec.TimeNS = int64(binary.LittleEndian.Uint64(p[8:16]))
	p = p[16:]
	get16 := func() (int, []byte, error) {
		if len(p) < 2 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint16(p[:2]))
		p = p[2:]
		if len(p) < n {
			return 0, nil, io.ErrUnexpectedEOF
		}
		out := p[:n]
		p = p[n:]
		return n, out, nil
	}
	_, key, err := get16()
	if err != nil {
		return rec, err
	}
	rec.Key = string(key)
	if len(p) < 2 {
		return rec, io.ErrUnexpectedEOF
	}
	hdrN := int(binary.LittleEndian.Uint16(p[:2]))
	p = p[2:]
	rec.Headers = make(map[string]string, hdrN)
	for i := 0; i < hdrN; i++ {
		_, k, err := get16()
		if err != nil {
			return rec, err
		}
		_, v, err := get16()
		if err != nil {
			return rec, err
		}
		rec.Headers[string(k)] = string(v)
	}
	if len(p) < 4 {
		return rec, io.ErrUnexpectedEOF
	}
	bodyN := int(binary.LittleEndian.Uint32(p[:4]))
	p = p[4:]
	if len(p) < bodyN {
		return rec, io.ErrUnexpectedEOF
	}
	rec.Body = append([]byte(nil), p[:bodyN]...)
	return rec, nil
}

// append persists rec with its assigned offset, fsyncs, and returns it.
func (l *recordLog) append(rec record) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec.Offset = l.endOff
	payload := encodePayload(rec)
	if len(payload)+4 > maxFrameLen {
		return 0, fmt.Errorf("mq: record too large")
	}
	var head [frameHeaderLen]byte
	// frameLen is the number of payload bytes following the fixed header;
	// the CRC of those bytes lives in head[8:12].
	binary.LittleEndian.PutUint64(head[0:8], uint64(len(payload)))
	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(head[8:12], crc)
	// recover() leaves the cursor at EOF; seek explicitly so the recorded
	// byte position is correct regardless of prior cursor state.
	pos, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := l.f.Write(head[:]); err != nil {
		return 0, err
	}
	if _, err := l.f.Write(payload); err != nil {
		return 0, err
	}
	if err := l.f.Sync(); err != nil {
		return 0, err
	}
	l.pos[rec.Offset] = pos
	l.endOff++
	return rec.Offset, nil
}

func (l *recordLog) read(offset int64) (record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pos, ok := l.pos[offset]
	if !ok {
		return record{}, fmt.Errorf("%w: offset %d", ErrNoMessage, offset)
	}
	var head [frameHeaderLen]byte
	if _, err := l.f.ReadAt(head[:], pos); err != nil {
		return record{}, err
	}
	frameLen := int64(binary.LittleEndian.Uint64(head[0:8]))
	payload := make([]byte, frameLen)
	if _, err := l.f.ReadAt(payload, pos+frameHeaderLen); err != nil {
		return record{}, err
	}
	wantCRC := binary.LittleEndian.Uint32(head[8:12])
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return record{}, fmt.Errorf("mq: crc mismatch at offset %d", offset)
	}
	return decodePayload(payload)
}

func (l *recordLog) end() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.endOff
}

func (l *recordLog) size() (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fi, err := l.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (l *recordLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
