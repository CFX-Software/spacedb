package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/inkwell/spacedb/core/internal/cache"
)

// invalidationEvent is the unsolicited frame format. Distinguished from
// a transportResponse by the empty ID and non-empty Event field; the JS
// bridge routes on Event.
type invalidationEvent struct {
	ID    string `json:"id,omitempty"`
	Event string `json:"event"`
	Table string `json:"table"`
	Key   string `json:"key,omitempty"`
}

// maybeInvalidateCache is called after every successful transport request.
// For write ops (execute / executeMany / transaction) it parses the SQL and
// drops cached rows that the write may have made stale. The parser is
// conservative: unfamiliar shapes drop the entire table.
func (s *Server) maybeInvalidateCache(req transportRequest) {
	switch req.Op {
	case "execute":
		s.applyParse(cache.Parse(req.Query, req.Params))
	case "executeMany":
		// Same SQL across many param sets. If the parser pins individual
		// rows, evict each; if it falls back to FullTable, one call suffices.
		if len(req.Rows) == 0 {
			s.applyParse(cache.Parse(req.Query, nil))
			return
		}
		first := cache.Parse(req.Query, asInterfaceSlice(req.Rows[0]))
		if first.FullTable != "" || first.NoOp {
			s.applyParse(first)
			return
		}
		for _, row := range req.Rows {
			s.applyParse(cache.Parse(req.Query, asInterfaceSlice(row)))
		}
	case "transaction":
		for _, step := range req.Steps {
			s.applyParse(cache.Parse(step.Query, step.Params))
		}
	}
}

func (s *Server) applyParse(r cache.ParseResult) {
	if r.NoOp {
		return
	}
	if r.FullTable != "" {
		s.cache.InvalidateTable(r.FullTable)
		s.broadcastInvalidation(r.FullTable, "")
		s.hub.NotifyTable(r.FullTable)
		return
	}
	for _, e := range r.Entries {
		s.cache.Invalidate(e.Table, e.Key)
		s.broadcastInvalidation(e.Table, e.Key)
		s.hub.NotifyTable(e.Table)
	}
}

func (s *Server) broadcastInvalidation(table, key string) {
	event := invalidationEvent{Event: "invalidate", Table: table, Key: key}
	s.subsMu.RLock()
	subs := make([]*transportSub, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	s.subsMu.RUnlock()
	for _, sub := range subs {
		sub.writeMu.Lock()
		_ = sub.encoder.Encode(event)
		sub.writeMu.Unlock()
	}
}

func asInterfaceSlice(row []interface{}) []interface{} {
	return row
}

// cacheInvalidate drops a single entry, or the whole table when key is empty.
func (s *Server) cacheInvalidate(req transportRequest) (interface{}, time.Duration, error) {
	if req.Table == "" {
		return nil, 0, errors.New("cacheInvalidate requires table")
	}
	if !validIdent.MatchString(req.Table) {
		return nil, 0, fmt.Errorf("invalid table=%q", req.Table)
	}
	if req.Key == "" {
		s.cache.InvalidateTable(req.Table)
	} else {
		s.cache.Invalidate(req.Table, req.Key)
	}
	s.broadcastInvalidation(req.Table, req.Key)
	return map[string]interface{}{"ok": true}, 0, nil
}
