package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/inkwell/spacedb/core/internal/cache"
	"github.com/inkwell/spacedb/core/internal/config"
	"github.com/inkwell/spacedb/core/internal/db"
	"github.com/inkwell/spacedb/core/internal/realtime"
)

// validIdent guards SQL identifiers we splice into queries (table and PK
// column names from the cacheGet/cacheSet payload). Anything that does
// not match is rejected before reaching the database driver.
var validIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Server struct {
	cfg   config.Config
	store *db.Store
	hub   *realtime.Hub
	cache *cache.Cache
	http  *http.Server
	tcp   net.Listener

	subsMu sync.RWMutex
	subs   map[*transportSub]struct{}
}

// transportSub wraps a single TCP transport connection so the server can
// fan out unsolicited events (cache invalidations) without racing the
// per-conn response writer.
type transportSub struct {
	encoder *json.Encoder
	writeMu *sync.Mutex
}

type queryRequest struct {
	Query  string        `json:"query"`
	Params []interface{} `json:"params"`
}

type prepareRequest struct {
	Name string      `json:"name"`
	SQL  string      `json:"sql"`
	Opts interface{} `json:"options"`
}

type transactionRequest struct {
	Steps []db.Step `json:"steps"`
}

type subRequest struct {
	ID     string        `json:"id"`
	Query  string        `json:"query"`
	Params []interface{} `json:"params"`
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

func New(cfg config.Config) (*Server, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout())
	defer cancel()

	store, err := db.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}

	s := &Server{
		cfg:   cfg,
		store: store,
		subs:  make(map[*transportSub]struct{}),
	}
	s.hub = realtime.New(store, cfg.PollInterval(), cfg.QueryTimeout())
	s.cache = cache.New(cache.Options{MaxEntries: 100_000})
	mux := http.NewServeMux()
	s.routes(mux)
	s.http = &http.Server{
		Addr:              cfg.Listen,
		Handler:           withCORS(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	errs := make(chan error, 1)
	go func() {
		slog.Info("spacedb core listening", "addr", s.cfg.Listen, "driver", s.cfg.Database.Driver)
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()
	go func() {
		if err := s.runTransport(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
			errs <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if s.tcp != nil {
			_ = s.tcp.Close()
		}
		_ = s.http.Shutdown(shutdownCtx)
		return s.store.Close()
	case err := <-errs:
		_ = s.store.Close()
		return err
	}
}

func (s *Server) runTransport(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Transport.Listen)
	if err != nil {
		return err
	}
	s.tcp = ln
	slog.Info("spacedb transport listening", "addr", s.cfg.Transport.Listen)

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
	}()
	var wg sync.WaitGroup
	defer wg.Wait()

	for scanner.Scan() {
		var req transportRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			writeMu.Lock()
			_ = encoder.Encode(transportResponse{OK: false, Error: err.Error()})
			writeMu.Unlock()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			response := s.handleTransportRequest(req)
			writeMu.Lock()
			_ = encoder.Encode(response)
			writeMu.Unlock()
		}()
	}
}

func (s *Server) handleTransportRequest(req transportRequest) transportResponse {
	tRecv := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.QueryTimeout())
	defer cancel()

	tDispatch := time.Now()
	result, dur, err := s.dispatchTransport(ctx, req)
	tDone := time.Now()
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

func (s *Server) dispatchTransport(ctx context.Context, req transportRequest) (interface{}, time.Duration, error) {
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
		return map[string]interface{}{"id": s.hub.Subscribe(context.Background(), req.Query, req.Params)}, 0, nil
	case "unsubscribe":
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

// cacheGet returns a cached row when present, otherwise fetches it from the
// database via `SELECT * FROM <table> WHERE <pk_column> = ?` and caches the
// result. Validates table/pkColumn against an identifier regex to keep this
// from being a SQL injection sink.
func (s *Server) cacheGet(ctx context.Context, req transportRequest) (interface{}, time.Duration, error) {
	if req.Table == "" || req.Key == "" {
		return nil, 0, errors.New("cacheGet requires table and key")
	}
	pkColumn := req.PKColumn
	if pkColumn == "" {
		pkColumn = "id"
	}
	if !validIdent.MatchString(req.Table) || !validIdent.MatchString(pkColumn) {
		return nil, 0, fmt.Errorf("invalid identifier table=%q pkColumn=%q", req.Table, pkColumn)
	}

	if row, ok := s.cache.Get(req.Table, req.Key); ok {
		return map[string]interface{}{"row": row, "cached": true}, 0, nil
	}

	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", req.Table, pkColumn)
	row, dur, err := s.store.Single(ctx, sql, []interface{}{req.Key})
	if err != nil {
		return nil, dur, err
	}
	if row != nil {
		s.cache.Set(req.Table, req.Key, row)
	}
	return map[string]interface{}{"row": row, "cached": false}, dur, nil
}

// cacheSet updates the cache entry. Callers are responsible for persisting
// the row via execute/insert/update; Phase 3 will auto-invalidate on those
// paths. For now we keep the cache layer SQL-free.
func (s *Server) cacheSet(req transportRequest) (interface{}, time.Duration, error) {
	if req.Table == "" || req.Key == "" {
		return nil, 0, errors.New("cacheSet requires table and key")
	}
	if req.Row == nil {
		return nil, 0, errors.New("cacheSet requires row")
	}
	if !validIdent.MatchString(req.Table) {
		return nil, 0, fmt.Errorf("invalid table=%q", req.Table)
	}
	s.cache.Set(req.Table, req.Key, req.Row)
	return map[string]interface{}{"ok": true}, 0, nil
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
		return
	}
	for _, e := range r.Entries {
		s.cache.Invalidate(e.Table, e.Key)
		s.broadcastInvalidation(e.Table, e.Key)
	}
}

// invalidationEvent is the unsolicited frame format. Distinguished from
// a transportResponse by the empty ID and non-empty Event field; the JS
// bridge routes on Event.
type invalidationEvent struct {
	ID    string `json:"id,omitempty"`
	Event string `json:"event"`
	Table string `json:"table"`
	Key   string `json:"key,omitempty"`
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

func (s *Server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/v1/query", s.query)
	mux.HandleFunc("/v1/single", s.single)
	mux.HandleFunc("/v1/execute", s.execute)
	mux.HandleFunc("/v1/prepare", s.prepare)
	mux.HandleFunc("/v1/transaction", s.transaction)
	mux.HandleFunc("/v1/subscribe", s.subscribe)
	mux.HandleFunc("/v1/unsubscribe", s.unsubscribe)
	mux.HandleFunc("/v1/events", s.events)
	mux.HandleFunc("/v1/stats", s.stats)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "driver": s.cfg.Database.Driver})
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if !decode(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout())
	defer cancel()
	rows, dur, err := s.store.Query(ctx, req.Query, req.Params)
	s.logQuery("query", req.Query, dur, err)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (s *Server) single(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if !decode(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout())
	defer cancel()
	row, dur, err := s.store.Single(ctx, req.Query, req.Params)
	s.logQuery("single", req.Query, dur, err)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, row)
}

func (s *Server) execute(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if !decode(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout())
	defer cancel()
	result, dur, err := s.store.Execute(ctx, req.Query, req.Params)
	s.logQuery("execute", req.Query, dur, err)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) prepare(w http.ResponseWriter, r *http.Request) {
	var req prepareRequest
	if !decode(w, r, &req) {
		return
	}
	if err := s.store.Prepare(req.Name, req.SQL); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "name": req.Name})
}

func (s *Server) transaction(w http.ResponseWriter, r *http.Request) {
	var req transactionRequest
	if !decode(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout())
	defer cancel()
	result, dur, err := s.store.Transaction(ctx, req.Steps)
	s.logQuery("transaction", "transaction", dur, err)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) subscribe(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.Realtime.Enabled {
		writeError(w, errors.New("realtime subscriptions are disabled"))
		return
	}
	var req subRequest
	if !decode(w, r, &req) {
		return
	}
	id := s.hub.Subscribe(context.Background(), req.Query, req.Params)
	writeJSON(w, http.StatusOK, map[string]interface{}{"id": id})
}

func (s *Server) unsubscribe(w http.ResponseWriter, r *http.Request) {
	var req subRequest
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": s.hub.Unsubscribe(req.ID)})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": s.hub.Events(id)})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"db":            s.store.Stats(),
		"subscriptions": s.hub.Count(),
	})
}

func (s *Server) logQuery(kind, query string, dur time.Duration, err error) {
	if err != nil {
		slog.Warn("query failed", "kind", kind, "durationMs", dur.Milliseconds(), "error", err)
		return
	}
	if dur >= s.cfg.SlowQueryThreshold() {
		slog.Warn("slow query", "kind", kind, "durationMs", dur.Milliseconds(), "query", query)
	}
}

func decode(w http.ResponseWriter, r *http.Request, out interface{}) bool {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return false
	}
	if r.Body == nil {
		writeError(w, errors.New("missing request body"))
		return false
	}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		writeError(w, err)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
