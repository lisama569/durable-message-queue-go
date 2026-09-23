package mq_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/durable-message-queue-go/mq"
)

// TestStressConcurrentConsume 高并发压力：
// 8 分区 × 50 条，6 个消费者并发拉取/确认，校验每条恰好处理一次。
func TestStressConcurrentConsume(t *testing.T) {
	cfg := withLogger(testConfig(t))
	cfg.PartCapacity = 10000
	b, _ := mq.NewBroker(cfg)
	defer b.Close()
	const parts, perPart, workers = 8, 50, 6
	if err := b.CreateTopic("evt", parts); err != nil {
		t.Fatal(err)
	}
	type id struct {
		p int
		o int64
	}
	total := parts * perPart
	for p := 0; p < parts; p++ {
		for i := 0; i < perPart; i++ {
			if _, err := b.Produce(mq.TopicPartition{Topic: "evt", Partition: p},
				mq.Message{Value: []byte("e")}); err != nil {
				t.Fatal(err)
			}
		}
	}
	consumers := make([]*mq.Consumer, workers)
	for i := 0; i < workers; i++ {
		c, err := b.JoinGroup("evt-g", "w"+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		consumers[i] = c
	}

	var mu sync.Mutex
	count := map[id]int{}
	acked := 0
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for _, c := range consumers {
		cons := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			empty := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// 模拟消费客户端的心跳线程：每轮拉取前续租。
				_ = b.Heartbeat(cons)
				ds, err := b.Poll(cons, 16)
				if err != nil {
					t.Errorf("poll: %v", err)
					return
				}
				if len(ds) == 0 {
					empty++
					if empty > 20 {
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				empty = 0
				for _, d := range ds {
					key := id{d.Record.Partition, d.Record.Offset}
					mu.Lock()
					count[key]++
					mu.Unlock()
					if err := b.Ack(cons, d); err != nil {
						t.Errorf("ack %s: %v", d.Record.MessageID(), err)
					}
					mu.Lock()
					acked++
					done := acked == total
					mu.Unlock()
					if done {
						close(stop)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if len(count) != total {
		t.Fatalf("唯一处理数=%d want %d（存在丢失或重复）", len(count), total)
	}
	for k, n := range count {
		if n != 1 {
			t.Fatalf("分区%d 位移%d 被处理 %d 次", k.p, k.o, n)
		}
	}
	t.Logf("判定: 压力场景 %d 条消息由 %d 个消费者并发处理，每条恰好一次（不丢不重）",
		total, workers)
}
