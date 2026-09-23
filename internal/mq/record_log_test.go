package mq

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecordLogAppendReadAndRecovery(t *testing.T) {
	dir := t.TempDir()
	log, err := openRecordLog(filepath.Join(dir, "logs"), "t", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want := []record{
		{Offset: -1, TimeNS: 100, Key: "k1", Headers: map[string]string{"a": "b"}, Body: []byte("hello")},
		{Offset: -1, TimeNS: 200, Key: "", Headers: map[string]string{}, Body: []byte("world!!")},
		{Offset: -1, TimeNS: 300, Key: "k3", Headers: nil, Body: []byte{0, 1, 2, 3}},
	}
	for i := range want {
		off, err := log.append(want[i])
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if off != int64(i) {
			t.Fatalf("offset = %d want %d", off, i)
		}
		t.Logf("APPEND topic=t partition=0 offset=%d key=%q criterion=sequential-offset-assignment", off, want[i].Key)
	}
	if err := log.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: index must be rebuilt from disk with identical contents.
	log2, err := openRecordLog(filepath.Join(dir, "logs"), "t", 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.close()
	if got := log2.end(); got != 3 {
		t.Fatalf("recovered end = %d want 3", got)
	}
	for i, w := range want {
		r, err := log2.read(int64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(r.Body) != string(w.Body) || r.Key != w.Key || r.TimeNS != w.TimeNS {
			t.Fatalf("record %d mismatch: %+v", i, r)
		}
		t.Logf("RECOVER offset=%d key=%q body=%q criterion=byte-identical-after-restart", i, r.Key, string(r.Body))
	}
}

func TestRecordLogTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	logs := filepath.Join(dir, "logs")
	log, err := openRecordLog(logs, "t", 1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := log.append(record{TimeNS: int64(i), Body: []byte("payload")}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	log.close()

	path := logPath(logs, "t", 1)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a torn write: append 7 garbage bytes past the valid tail.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("garbage")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	log2, err := openRecordLog(logs, "t", 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.close()
	if got := log2.end(); got != 3 {
		t.Fatalf("end after torn write = %d want 3 (torn tail must be discarded)", got)
	}
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Fatalf("file size = %d want %d (torn bytes must be truncated)", after.Size(), before.Size())
	}
	t.Logf("RECOVER partition=1 offset-end=%d criterion=torn-tail-truncated-no-data-loss", log2.end())
}
