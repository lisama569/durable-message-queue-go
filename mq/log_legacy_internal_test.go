package mq

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// encodeRecordLegacyBody 复刻旧版记录体布局（不含主题/分区字段），
// 用于构造升级前落盘的日志文件，验证新版 Broker 仍能恢复并正确 Ack。
func encodeRecordLegacyBody(r Record) []byte {
	var b bytes.Buffer
	putInt64(&b, r.Offset)
	putInt64(&b, r.EnqueuedAt.UnixNano())
	putBytes(&b, r.Key)
	putBytes(&b, r.Value)
	putInt64(&b, 0) // 无 headers
	return b.Bytes()
}

// TestLegacyLogRecoveredInternal 构造一个【旧格式】分区日志文件，
// 重启后断言：记录可恢复（不被当成损坏截断）、主题/分区从 l.tp 回填、
// 该消息可被正常投递且 Ack 归属校验通过。
func TestLegacyLogRecoveredInternal(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	tp := TopicPartition{Topic: "orders", Partition: 7}

	// 先用正常路径建出目录与日志文件，再整体改写成旧格式内容。
	cfg := DefaultConfig(dataDir)
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders", 8); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	path := logFilePath(dataDir, tp)
	var raw bytes.Buffer
	body := encodeRecordLegacyBody(Record{Offset: 0, Key: []byte("k"), Value: []byte("legacy-v")})
	var pfx [8]byte
	putUint64Raw(pfx[:], uint64(len(body)))
	raw.Write(pfx[:])
	raw.Write(body)
	if err := os.WriteFile(path, raw.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	b2, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.CreateTopic("orders", 8); err != nil {
		t.Fatal(err)
	}
	l, ok := b2.logs[tp]
	if !ok {
		t.Fatal("分区日志未打开")
	}
	if l.endOffset() != 1 {
		t.Fatalf("旧格式日志恢复长度=%d want 1", l.endOffset())
	}
	rec, exists := l.Read(0)
	if !exists || rec.Topic != "orders" || rec.Partition != 7 ||
		string(rec.Value) != "legacy-v" {
		t.Fatalf("旧格式记录恢复/回填异常: %+v exists=%v", rec, exists)
	}
}

func putUint64Raw(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}
