package mq

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }
func (c *fakeClock) now() time.Time {
	return c.t
}
func (c *fakeClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func testConfig(t *testing.T) BrokerConfig {
	t.Helper()
	clk := newFakeClock()
	t.Setenv("MQ_TEST_CLOCK", "")
	return BrokerConfig{
		RootDir:        t.TempDir(),
		Partitions:     2,
		MaxRetries:     2,
		LeaseTTL:       100 * time.Millisecond,
		Backoff:        ConstantBackoff{D: 50 * time.Millisecond},
		Now:            clk.now,
		FetchTimeout:   time.Second,
		ProduceTimeout: time.Second,
	}
}

func testConfigReal(t *testing.T) BrokerConfig {
	t.Helper()
	return BrokerConfig{
		RootDir:        t.TempDir(),
		Partitions:     4,
		MaxRetries:     2,
		LeaseTTL:       200 * time.Millisecond,
		Backoff:        ConstantBackoff{D: 20 * time.Millisecond},
		Now:            time.Now,
		FetchTimeout:   time.Second,
		ProduceTimeout: time.Second,
	}
}

func mustOpen(t *testing.T, cfg BrokerConfig) *Broker {
	t.Helper()
	b, err := Open(cfg)
	if err != nil {
		t.Fatalf("open broker: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.CreateTopic("orders"); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return b
}
