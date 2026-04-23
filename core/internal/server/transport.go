package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/inkwell/spacedb/core/internal/db"
	"github.com/inkwell/spacedb/core/internal/realtime"
)

// transportSub wraps a single TCP transport connection so the server can
// fan out unsolicited events (cache invalidations, realtime subscription
// changes) without racing the per-conn response writer.
type transportSub struct {
	encoder  *json.Encoder
	writeMu  *sync.Mutex
	subIDsMu sync.Mutex
	subIDs   []string
}

// subscriptionEvent is the unsolicited frame for realtime subscription
// updates. Distinguished from a transportResponse by the empty top-level
// ID and the Event field. The JS bridge routes on Event.
type subscriptionEvent struct {
	Event     string                   `json:"event"`
	SubID     string                   `json:"subId"`
	Type      string                   `json:"type"`
	Query     string                   `json:"query,omitempty"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Error     string                   `json:"error,omitempty"`
	CreatedAt int64                    `json:"createdAt"`
}

type transportRequest struct {
	ID       string                 `json:"id"`
	Op       string                 `json:"op"`
	SubID    string                 `json:"subId"`
	Query    string                 `json:"query"`
	Params   []interface{}          `json:"params"`
	Rows     [][]interface{}        `json:"rows"`
	Name     string                 `json:"name"`
	SQL      string                 `json:"sql"`
	Steps    []db.Step              `json:"steps"`
	Profile  bool                   `json:"profile,omitempty"`
	Table    string                 `json:"table,omitempty"`
	Key      string                 `json:"key,omitempty"`
	Keys     []interface{}          `json:"keys,omitempty"`
	PKColumn string                 `json:"pkColumn,omitempty"`
	Row      map[string]interface{} `json:"row,omitempty"`
}

type transportProfile struct {
	ServerTotalNs int64 `json:"serverTotalNs"`
	DispatchNs    int64 `json:"dispatchNs"`
	DbDurNs       int64 `json:"dbDurNs"`
}

type transportResponse struct {
	ID      string            `json:"id"`
	OK      bool              `json:"ok"`
	Result  interface{}       `json:"result,omitempty"`
	Error   string            `json:"error,omitempty"`
	Profile *transportProfile `json:"profile,omitempty"`
}

func (s *Server) runTransport(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Transport.Listen)
	if err != nil {
		return err
	}
	s.tcp = ln

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleTransportConn(conn)
	}
}

func (s *Server) handleTransportConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	encoder := json.NewEncoder(conn)
	var writeMu sync.Mutex
	sub := &transportSub{encoder: encoder, writeMu: &writeMu}
	s.subsMu.Lock()
	s.subs[sub] = struct{}{}
	s.subsMu.Unlock()
	defer func() {
		s.subsMu.Lock()
		delete(s.subs, sub)
		s.subsMu.Unlock()
		// Tear down any realtime subscriptions this connection owned so
		// the hub goroutines exit and don't push into a dead encoder.
		sub.subIDsMu.Lock()
		ids := sub.subIDs
		sub.subIDs = nil
		sub.subIDsMu.Unlock()
		for _, id := range ids {
			s.hub.Unsubscribe(id)
		}
	}()
	var wg sync.WaitGroup
	defer wg.Wait()

	// Bound in-flight handlers per connection. Without this, a misbehaving
	// or malicious client can drive arbitrary goroutine growth by firing
	// requests faster than the DB can respond. The scanner read pauses
	// while the slot channel is full, applying backpressure to the socket.
	const maxInFlightPerConn = 128
	slots := make(chan struct{}, maxInFlightPerConn)

	for scanner.Scan() {
		var req transportRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			writeMu.Lock()
			_ = encoder.Encode(transportResponse{OK: false, Error: err.Error()})
			writeMu.Unlock()
			continue
		}

		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			response := s.handleTransportRequest(req, sub)
			writeMu.Lock()
			_ = encoder.Encode(response)
			writeMu.Unlock()
		}()
	}
}

func (s *Server) handleTransportRequest(req transportRequest, sub *transportSub) transportResponse {
	tRecv := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.QueryTimeout())
	defer cancel()

	tDispatch := time.Now()
	result, dur, err := s.dispatchTransport(ctx, req, sub)
	tDone := time.Now()
	s.metrics.record(req.Op, tDone.Sub(tDispatch), err)
	s.logQuery(req.Op, req.Query, dur, err)
	if err == nil {
		s.maybeInvalidateCache(req)
	}

	var profile *transportProfile
	if req.Profile {
		profile = &transportProfile{
			ServerTotalNs: tDone.Sub(tRecv).Nanoseconds(),
			DispatchNs:    tDone.Sub(tDispatch).Nanoseconds(),
			DbDurNs:       dur.Nanoseconds(),
		}
	}

	if err != nil {
		return transportResponse{ID: req.ID, OK: false, Error: err.Error(), Profile: profile}
	}
	return transportResponse{ID: req.ID, OK: true, Result: result, Profile: profile}
}

func (s *Server) dispatchTransport(ctx context.Context, req transportRequest, sub *transportSub) (interface{}, time.Duration, error) {
	switch req.Op {
	case "health":
		return map[string]interface{}{"ok": true, "driver": s.cfg.Database.Driver}, 0, nil
	case "stats":
		return map[string]interface{}{
			"db":            s.store.Stats(),
			"subscriptions": s.hub.Count(),
		}, 0, nil
	case "subscribe":
		if !s.cfg.Realtime.Enabled {
			return nil, 0, errors.New("realtime subscriptions are disabled")
		}
		// On TCP, attach a sink that pushes events back over the same
		// socket. Tracks the sub ID against the connection so disconnect
		// tears down the hub goroutine.
		var opts []realtime.SubOption
		if sub != nil {
			sink := func(e realtime.Event) {
				frame := subscriptionEvent{
					Event:     "subscription",
					SubID:     e.ID,
					Type:      e.Type,
					Query:     e.Query,
					Rows:      e.Rows,
					Error:     e.Error,
					CreatedAt: e.CreatedAt,
				}
				sub.writeMu.Lock()
				_ = sub.encoder.Encode(frame)
				sub.writeMu.Unlock()
			}
			opts = append(opts, realtime.WithSink(sink))
		}
		id := s.hub.Subscribe(context.Background(), req.Query, req.Params, opts...)
		if sub != nil {
			sub.subIDsMu.Lock()
			sub.subIDs = append(sub.subIDs, id)
			sub.subIDsMu.Unlock()
		}
		return map[string]interface{}{"id": id}, 0, nil
	case "unsubscribe":
		if sub != nil {
			sub.subIDsMu.Lock()
			for i, id := range sub.subIDs {
				if id == req.SubID {
					sub.subIDs = append(sub.subIDs[:i], sub.subIDs[i+1:]...)
					break
				}
			}
			sub.subIDsMu.Unlock()
		}
		return map[string]interface{}{"ok": s.hub.Unsubscribe(req.SubID)}, 0, nil
	case "events":
		return map[string]interface{}{"events": s.hub.Events(req.SubID)}, 0, nil
	case "query":
		return s.store.Query(ctx, req.Query, req.Params)
	case "single":
		return s.store.Single(ctx, req.Query, req.Params)
	case "execute":
		return s.store.Execute(ctx, req.Query, req.Params)
	case "executeMany":
		return s.store.ExecuteMany(ctx, req.Query, req.Rows)
	case "prepare":
		start := time.Now()
		return map[string]interface{}{"ok": true, "name": req.Name}, time.Since(start), s.store.Prepare(req.Name, req.SQL)
	case "transaction":
		return s.store.Transaction(ctx, req.Steps)
	case "cacheGet":
		return s.cacheGet(ctx, req)
	case "cacheGetMany":
		return s.cacheGetMany(ctx, req)
	case "cacheSet":
		return s.cacheSet(req)
	case "cacheInvalidate":
		return s.cacheInvalidate(req)
	case "cacheStats":
		return s.cache.Stats(), 0, nil
	default:
		return nil, 0, fmt.Errorf("unsupported transport op %q", req.Op)
	}
}
