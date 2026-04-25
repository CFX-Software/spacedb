package server

import (
	"sync"
	"sync/atomic"
	"time"
)

// opMetrics holds rolling counters for one transport op (query, execute,
// etc). Counters use atomics so the hot path stays lock-free; the ring
// buffer of recent latencies (for percentile computation) sits behind a
// small mutex because computing percentiles requires sorting a snapshot.
type opMetrics struct {
	count    atomic.Uint64
	errCount atomic.Uint64
	totalNs  atomic.Int64
	maxNs    atomic.Int64

	sampleMu sync.Mutex
	samples  []int64 // ring buffer of recent latency ns, capped
	sampleIx int     // next write position
}

const sampleCap = 256

func newOpMetrics() *opMetrics {
	return &opMetrics{samples: make([]int64, 0, sampleCap)}
}

func (m *opMetrics) record(dur time.Duration, err error) {
	ns := dur.Nanoseconds()
	m.count.Add(1)
	if err != nil {
		m.errCount.Add(1)
	}
	m.totalNs.Add(ns)
	for {
		prev := m.maxNs.Load()
		if ns <= prev || m.maxNs.CompareAndSwap(prev, ns) {
			break
		}
	}
	m.sampleMu.Lock()
	if len(m.samples) < sampleCap {
		m.samples = append(m.samples, ns)
	} else {
		m.samples[m.sampleIx] = ns
		m.sampleIx = (m.sampleIx + 1) % sampleCap
	}
	m.sampleMu.Unlock()
}

func (m *opMetrics) snapshot() map[string]interface{} {
	count := m.count.Load()
	total := m.totalNs.Load()
	max := m.maxNs.Load()
	out := map[string]interface{}{
		"count":    count,
		"errors":   m.errCount.Load(),
		"totalMs":  total / 1_000_000,
		"maxMs":    max / 1_000_000,
	}
	if count > 0 {
		out["avgMs"] = (total / int64(count)) / 1_000_000
	}
	m.sampleMu.Lock()
	if len(m.samples) > 0 {
		// Sort a copy to avoid disturbing the ring; len is bounded so this
		// is cheap.
		sorted := make([]int64, len(m.samples))
		copy(sorted, m.samples)
		insertionSort(sorted)
		out["p50Ms"] = sorted[len(sorted)/2] / 1_000_000
		out["p95Ms"] = sorted[(len(sorted)*95)/100] / 1_000_000
		out["p99Ms"] = sorted[(len(sorted)*99)/100] / 1_000_000
	}
	m.sampleMu.Unlock()
	return out
}

// insertionSort beats stdlib sort.Slice for small N (256 max here) because
// it avoids the closure allocation and reflection path.
func insertionSort(a []int64) {
	for i := 1; i < len(a); i++ {
		v := a[i]
		j := i - 1
		for j >= 0 && a[j] > v {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = v
	}
}

// metricsByOp returns a metrics map keyed by op name. The map itself is
// fixed at startup so reads on the hot path don't need a lock.
type metricsByOp struct {
	ops map[string]*opMetrics
}

func newMetricsByOp() *metricsByOp {
	ops := map[string]*opMetrics{}
	for _, op := range []string{
		"query", "single", "execute", "executeMany", "prepare",
		"transaction", "subscribe", "unsubscribe", "events",
		"cacheGet", "cacheGetMany", "cacheSet", "cacheInvalidate",
		"cacheStats", "health", "stats",
	} {
		ops[op] = newOpMetrics()
	}
	return &metricsByOp{ops: ops}
}

func (m *metricsByOp) record(op string, dur time.Duration, err error) {
	if om, ok := m.ops[op]; ok {
		om.record(dur, err)
	}
}

func (m *metricsByOp) snapshot() map[string]interface{} {
	out := make(map[string]interface{}, len(m.ops))
	for name, om := range m.ops {
		if om.count.Load() == 0 {
			continue
		}
		out[name] = om.snapshot()
	}
	return out
}
