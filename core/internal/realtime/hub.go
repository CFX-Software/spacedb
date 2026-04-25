package realtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/inkwell/spacedb/core/internal/db"
)

type Queryer interface {
	Query(ctx context.Context, query string, params []interface{}) ([]map[string]interface{}, time.Duration, error)
}

type Event struct {
	ID        string                   `json:"id"`
	Type      string                   `json:"type"`
	Query     string                   `json:"query"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Error     string                   `json:"error,omitempty"`
	CreatedAt int64                    `json:"createdAt"`
}

type Hub struct {
	store    Queryer
	interval time.Duration
	timeout  time.Duration
	mu       sync.Mutex
	nextID   int64
	subs     map[string]*subscription
}

type subscription struct {
	id     string
	query  string
	params []interface{}
	tables []string // lowercased tables the SELECT touches; used by NotifyTable
	hash   string
	events []Event
	cancel context.CancelFunc
	sink   func(Event)
}

// fromTableRe finds table names after FROM/JOIN. Backticks are stripped
// before matching (MySQL identifier wrapper). For Postgres double-quoted
// identifiers callers should pass them unquoted in subscription SQL — the
// hub's invalidation hook is best-effort and falls back to polling.
var fromTableRe = regexp.MustCompile(`(?i)(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)`)

func extractTables(query string) []string {
	cleaned := strings.ReplaceAll(query, "`", "")
	matches := fromTableRe.FindAllStringSubmatch(cleaned, -1)
	seen := map[string]bool{}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		t := strings.ToLower(m[1])
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// SubOption configures a subscription at creation time.
type SubOption func(*subscription)

// WithSink delivers events synchronously to sink instead of buffering them
// for poll-based retrieval via Events(). Used by the TCP transport to push
// events back over the same socket without going through /events polling.
func WithSink(sink func(Event)) SubOption {
	return func(s *subscription) { s.sink = sink }
}

func New(store Queryer, interval, timeout time.Duration) *Hub {
	return &Hub{store: store, interval: interval, timeout: timeout, subs: map[string]*subscription{}}
}

func (h *Hub) Subscribe(parent context.Context, query string, params []interface{}, opts ...SubOption) string {
	h.mu.Lock()
	h.nextID++
	id := fmt.Sprintf("sub_%s_%d", time.Now().Format("20060102150405"), h.nextID)
	ctx, cancel := context.WithCancel(parent)
	sub := &subscription{
		id:     id,
		query:  query,
		params: params,
		tables: extractTables(query),
		cancel: cancel,
	}
	for _, opt := range opts {
		opt(sub)
	}
	h.subs[id] = sub
	h.mu.Unlock()

	go h.run(ctx, sub)
	return id
}

func (h *Hub) Unsubscribe(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := h.subs[id]
	if !ok {
		return false
	}
	sub.cancel()
	delete(h.subs, id)
	return true
}

func (h *Hub) Events(id string) []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := h.subs[id]
	if !ok || len(sub.events) == 0 {
		return []Event{}
	}
	events := sub.events
	sub.events = nil
	return events
}

func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// NotifyTable wakes every subscription that reads `table` so it re-runs
// its query right away rather than waiting on the next poll tick. Wired
// from the server's cache invalidation path so writes routed through
// spacedb deliver sub events in single-digit ms instead of poll-interval
// ms. External writes (other resources hitting MySQL directly) still rely
// on the polling backstop.
func (h *Hub) NotifyTable(table string) {
	if table == "" {
		return
	}
	target := strings.ToLower(table)
	h.mu.Lock()
	matching := make([]*subscription, 0)
	for _, sub := range h.subs {
		for _, t := range sub.tables {
			if t == target {
				matching = append(matching, sub)
				break
			}
		}
	}
	h.mu.Unlock()
	for _, sub := range matching {
		// Fire-and-forget check; each runs in its own goroutine so a slow
		// DB query doesn't stall the notifier. hash dedup inside check()
		// keeps duplicate ticker/notify races from over-pushing.
		go h.check(context.Background(), sub)
	}
}

func (h *Hub) run(ctx context.Context, sub *subscription) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	h.check(ctx, sub)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.check(ctx, sub)
		}
	}
}

func (h *Hub) check(ctx context.Context, sub *subscription) {
	checkCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	rows, _, err := h.store.Query(checkCtx, sub.query, sub.params)
	event := Event{ID: sub.id, Query: sub.query, CreatedAt: time.Now().UnixMilli()}
	if err != nil {
		event.Type = "error"
		event.Error = err.Error()
		h.push(sub.id, event)
		return
	}

	hash := hashRows(rows)
	h.mu.Lock()
	changed := sub.hash != hash
	sub.hash = hash
	h.mu.Unlock()

	if changed {
		event.Type = "changed"
		event.Rows = rows
		h.push(sub.id, event)
	}
}

func (h *Hub) push(id string, event Event) {
	h.mu.Lock()
	sub, ok := h.subs[id]
	if !ok {
		h.mu.Unlock()
		return
	}
	sink := sub.sink
	if sink == nil {
		sub.events = append(sub.events, event)
		if len(sub.events) > 64 {
			sub.events = sub.events[len(sub.events)-64:]
		}
	}
	h.mu.Unlock()
	if sink != nil {
		sink(event)
	}
}

func hashRows(rows []map[string]interface{}) string {
	data, _ := json.Marshal(rows)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var _ Queryer = (*db.Store)(nil)
