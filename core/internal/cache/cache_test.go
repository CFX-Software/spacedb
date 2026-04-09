package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetGetRoundTrip(t *testing.T) {
	c := New(Options{})
	c.Set("users", "1", Row{"id": int64(1), "name": "Jane"})

	row, ok := c.Get("users", "1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if row["name"] != "Jane" {
		t.Fatalf("got %v want Jane", row["name"])
	}
}

func TestGetMiss(t *testing.T) {
	c := New(Options{})
	_, ok := c.Get("users", "missing")
	if ok {
		t.Fatal("expected miss on absent key")
	}
	stats := c.Stats()
	if stats.Misses != 1 {
		t.Fatalf("miss counter = %d want 1", stats.Misses)
	}
}

func TestInvalidateSingleKey(t *testing.T) {
	c := New(Options{})
	c.Set("users", "1", Row{"name": "Jane"})
	c.Invalidate("users", "1")
	if _, ok := c.Get("users", "1"); ok {
		t.Fatal("expected miss after invalidate")
	}
}

func TestInvalidateTable(t *testing.T) {
	c := New(Options{})
	for i := 0; i < 10; i++ {
		c.Set("users", fmt.Sprintf("%d", i), Row{"id": i})
		c.Set("posts", fmt.Sprintf("%d", i), Row{"id": i})
	}
	c.InvalidateTable("users")

	if _, ok := c.Get("users", "5"); ok {
		t.Fatal("users entry should be gone")
	}
	if _, ok := c.Get("posts", "5"); !ok {
		t.Fatal("posts entry should still be present")
	}
	if got := c.Stats().Entries; got != 10 {
		t.Fatalf("entries = %d want 10", got)
	}
}

func TestTTLExpiry(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	clock := &now
	c := New(Options{
		TTL: 5 * time.Second,
		Now: func() time.Time { return *clock },
	})
	c.Set("users", "1", Row{"name": "Jane"})

	if _, ok := c.Get("users", "1"); !ok {
		t.Fatal("expected hit immediately after set")
	}

	*clock = clock.Add(6 * time.Second)

	if _, ok := c.Get("users", "1"); ok {
		t.Fatal("expected miss after TTL expiry")
	}
	if got := c.Stats().Entries; got != 0 {
		t.Fatalf("entries = %d want 0 (expired entry should be deleted)", got)
	}
}

func TestLRUEvictionOnSizeCap(t *testing.T) {
	// MaxEntries 512 → per-shard cap of 2 entries. Insert 4 keys hashing
	// across many shards; eviction logic only fires when a single shard
	// exceeds its slice. We test the behavior directly by inserting many
	// keys with the same table and confirming Entries stays at the cap.
	c := New(Options{MaxEntries: 512})
	for i := 0; i < 5000; i++ {
		c.Set("hot", fmt.Sprintf("%d", i), Row{"i": i})
	}
	stats := c.Stats()
	if stats.Entries > 512+shardCount {
		// Allow a fudge: per-shard rounding can over-shoot by up to (shards - 1).
		t.Fatalf("entries = %d, well past cap=512", stats.Entries)
	}
	if stats.Evictions == 0 {
		t.Fatal("expected eviction counter to fire")
	}
}

func TestLRUTouchOnGet(t *testing.T) {
	c := New(Options{MaxEntries: shardCount * 2}) // cap=2 per shard
	// Force shard 0 by picking colliding keys. Use a simple loop until we
	// find two keys mapping to the same shard as "hot:a".
	target := c.shardFor("hot", "a")
	var keys []string
	for i := 0; len(keys) < 3; i++ {
		k := fmt.Sprintf("k%d", i)
		if c.shardFor("hot", k) == target {
			keys = append(keys, k)
		}
	}

	c.Set("hot", keys[0], Row{"i": 0})
	c.Set("hot", keys[1], Row{"i": 1})

	// Touch keys[0] so keys[1] becomes the LRU victim.
	if _, ok := c.Get("hot", keys[0]); !ok {
		t.Fatal("hit expected on keys[0]")
	}

	c.Set("hot", keys[2], Row{"i": 2})

	if _, ok := c.Get("hot", keys[0]); !ok {
		t.Fatal("keys[0] should survive (most-recently-touched)")
	}
	if _, ok := c.Get("hot", keys[1]); ok {
		t.Fatal("keys[1] should have been evicted")
	}
	if _, ok := c.Get("hot", keys[2]); !ok {
		t.Fatal("keys[2] should be present (just inserted)")
	}
}

func TestConcurrentSetGetSafety(t *testing.T) {
	c := New(Options{MaxEntries: 10_000})

	const workers = 32
	const opsPerWorker = 1_000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				k := fmt.Sprintf("%d-%d", id, i%100)
				c.Set("users", k, Row{"id": i})
				_, _ = c.Get("users", k)
			}
		}(w)
	}
	wg.Wait()

	stats := c.Stats()
	if stats.Hits == 0 {
		t.Fatal("expected at least some hits under concurrent set/get")
	}
}

func TestStatsCountersAccumulate(t *testing.T) {
	c := New(Options{})
	c.Set("t", "1", Row{"v": 1})

	for i := 0; i < 3; i++ {
		_, _ = c.Get("t", "1")
	}
	for i := 0; i < 5; i++ {
		_, _ = c.Get("t", "missing")
	}

	stats := c.Stats()
	if stats.Hits != 3 {
		t.Fatalf("hits = %d want 3", stats.Hits)
	}
	if stats.Misses != 5 {
		t.Fatalf("misses = %d want 5", stats.Misses)
	}
	if stats.Entries != 1 {
		t.Fatalf("entries = %d want 1", stats.Entries)
	}
}
