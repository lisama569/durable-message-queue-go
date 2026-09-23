package mq_test

import (
	"context"
	"fmt"
	"os"
	"time"

	mq "github.com/lisama569/durable-message-queue-go/internal/mq"
)

// Example shows the end-to-end lifecycle: open, produce, join a group,
// fetch/ack, and observe a committed offset.
func Example() {
	dir, err := os.MkdirTemp("", "mq-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	b, err := mq.Open(mq.BrokerConfig{
		RootDir:      dir,
		Partitions:   1,
		PartitionCap: 1000,
		MaxRetries:   3,
		LeaseTTL:     10 * time.Second,
		Backoff:      mq.LinearBackoff{Base: 100 * time.Millisecond},
	})
	if err != nil {
		panic(err)
	}
	defer b.Close()
	if err := b.CreateTopic("orders"); err != nil {
		panic(err)
	}

	ctx := context.Background()
	tp, off, err := b.Produce(ctx, "orders", mq.Message{Key: "user-1", Body: []byte("hello")})
	if err != nil {
		panic(err)
	}
	fmt.Printf("produced offset=%d on %s\n", off, tp)

	gen, assigned, err := b.JoinGroup("billing", "worker-1")
	if err != nil {
		panic(err)
	}
	d, err := b.Fetch(ctx, "billing", "worker-1", gen, assigned[0])
	if err != nil {
		panic(err)
	}
	fmt.Printf("delivered offset=%d body=%s\n", d.Message.Offset, string(d.Message.Body))
	if err := b.Ack("billing", "worker-1", gen, d); err != nil {
		panic(err)
	}
	st, _ := b.PartitionStats(tp.Topic, tp.Partition)
	fmt.Printf("committed=%d lag=%d\n", st.Committed["billing"], st.Lag)

	// On failure call b.Nack(...); after MaxRetries+1 failures the message
	// lands in b.DeadLetters() with its original reason and the offset moves
	// forward so the partition never stalls.
	_ = time.Millisecond

	// Output:
	// produced offset=0 on orders#0
	// delivered offset=0 body=hello
	// committed=1 lag=0
}
