package realtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	hash   string
	events []Event
	cancel context.CancelFunc
}

func New(store Queryer, interval, timeout time.Duration) *Hub {
	return &Hub{store: store, interval: interval, timeout: timeout, subs: map[string]*subscription{}}
}

func (h *Hub) Subscribe(parent context.Context, query string, params []interface{}) string {
	h.mu.Lock()
	h.nextID++
	id := fmt.Sprintf("sub_%s_%d", time.Now().Format("20060102150405"), h.nextID)
	ctx, cancel := context.WithCancel(parent)
	sub := &subscription{id: id, query: query, params: params, cancel: cancel}
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
	defer h.mu.Unlock()
	if sub, ok := h.subs[id]; ok {
		sub.events = append(sub.events, event)
		if len(sub.events) > 64 {
			sub.events = sub.events[len(sub.events)-64:]
		}
	}
}

func hashRows(rows []map[string]interface{}) string {
	data, _ := json.Marshal(rows)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var _ Queryer = (*db.Store)(nil)
