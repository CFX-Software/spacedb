// Package cache is the in-process read cache for spacedb. Row-level cache
// keyed by (table, primary key). Sharded for concurrent access. LRU
// eviction on size cap. Optional TTL.
//
// See PLAN-cache.md for the design + rationale.
package cache

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 256

// Row is the cached value. We use map[string]any so callers can stuff
// whatever the SQL driver produced (column-name to value) without taking
// a dependency on a concrete row type.
type Row = map[string]any

type entry struct {
	table     string
	key       string
	row       Row
	expiresAt time.Time
	elem      *list.Element
}

type shard struct {
	mu    sync.RWMutex
	items map[string]*entry
	lru   *list.List
}

// Options configures a Cache. All fields are optional.
type Options struct {
	MaxEntries int           // per-process cap. <=0 means unbounded.
	TTL        time.Duration // per-entry TTL. 0 means no expiry.
	Now        func() time.Time
}

// Cache is the public type. Safe for concurrent use.
type Cache struct {
	shards     [shardCount]*shard
	maxEntries int
	ttl        time.Duration
	now        func() time.Time

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// New constructs a cache with the given options.
func New(opts Options) *Cache {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	c := &Cache{
		maxEntries: opts.MaxEntries,
		ttl:        opts.TTL,
		now:        opts.Now,
	}
	for i := range c.shards {
		c.shards[i] = &shard{
			items: make(map[string]*entry),
			lru:   list.New(),
		}
	}
	return c
}

func (c *Cache) shardFor(table, key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(table))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	return c.shards[h.Sum32()%shardCount]
}

func cacheKey(table, key string) string {
	return table + "\x00" + key
}

// Get returns the cached row and true on hit. Expired entries return miss
// and are deleted as a side effect.
func (c *Cache) Get(table, key string) (Row, bool) {
	s := c.shardFor(table, key)
	k := cacheKey(table, key)

	s.mu.RLock()
	e, ok := s.items[k]
	s.mu.RUnlock()
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	if !e.expiresAt.IsZero() && c.now().After(e.expiresAt) {
		s.mu.Lock()
		// Re-check after grabbing the write lock; another goroutine may
		// have refreshed the entry between our RUnlock and Lock.
		if cur, stillThere := s.items[k]; stillThere && cur == e {
			delete(s.items, k)
			s.lru.Remove(e.elem)
		}
		s.mu.Unlock()
		c.misses.Add(1)
		return nil, false
	}

	s.mu.Lock()
	if cur, stillThere := s.items[k]; stillThere && cur == e {
		s.lru.MoveToFront(e.elem)
	}
	s.mu.Unlock()

	c.hits.Add(1)
	return e.row, true
}

// Set inserts or replaces the cached row.
func (c *Cache) Set(table, key string, row Row) {
	s := c.shardFor(table, key)
	k := cacheKey(table, key)

	var expires time.Time
	if c.ttl > 0 {
		expires = c.now().Add(c.ttl)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.items[k]; ok {
		e.row = row
		e.expiresAt = expires
		s.lru.MoveToFront(e.elem)
		return
	}

	e := &entry{table: table, key: key, row: row, expiresAt: expires}
	e.elem = s.lru.PushFront(e)
	s.items[k] = e

	if c.maxEntries > 0 {
		c.evictIfFull(s)
	}
}

// Invalidate drops a single entry.
func (c *Cache) Invalidate(table, key string) {
	s := c.shardFor(table, key)
	k := cacheKey(table, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[k]; ok {
		delete(s.items, k)
		s.lru.Remove(e.elem)
	}
}

// InvalidateTable drops every entry for a table across all shards.
// Used as the catch-all for writes whose target rows we can't pin down.
func (c *Cache) InvalidateTable(table string) {
	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.items {
			if e.table == table {
				delete(s.items, k)
				s.lru.Remove(e.elem)
			}
		}
		s.mu.Unlock()
	}
}

// evictIfFull removes the least-recently-used entry from this shard until
// the global cap is satisfied. Called with the shard write lock held.
func (c *Cache) evictIfFull(s *shard) {
	// Cheap per-shard upper bound: assume even distribution across 256 shards.
	// If the cache is hot on one table that hashes to one shard, this slightly
	// over-evicts that shard. Acceptable for v1.
	perShardCap := c.maxEntries / shardCount
	if perShardCap < 1 {
		perShardCap = 1
	}
	for s.lru.Len() > perShardCap {
		back := s.lru.Back()
		if back == nil {
			break
		}
		victim := back.Value.(*entry)
		delete(s.items, cacheKey(victim.table, victim.key))
		s.lru.Remove(back)
		c.evictions.Add(1)
	}
}

// Stats snapshot for the /stats endpoint.
type Stats struct {
	Entries   int    `json:"entries"`
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Evictions uint64 `json:"evictions"`
}

// Stats returns a point-in-time snapshot. Entries iterates all shards;
// hits/misses/evictions are atomic counters.
func (c *Cache) Stats() Stats {
	entries := 0
	for _, s := range c.shards {
		s.mu.RLock()
		entries += len(s.items)
		s.mu.RUnlock()
	}
	return Stats{
		Entries:   entries,
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
	}
}
