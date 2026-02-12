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
	"sync"
	"time"

	"github.com/inkwell/spacedb/core/internal/config"
	"github.com/inkwell/spacedb/core/internal/db"
	"github.com/inkwell/spacedb/core/internal/realtime"
)

type Server struct {
	cfg   config.Config
	store *db.Store
	hub   *realtime.Hub
	http  *http.Server
	tcp   net.Listener
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
	ID     string        `json:"id"`
	Op     string        `json:"op"`
	SubID  string        `json:"subId"`
	Query  string        `json:"query"`
	Params []interface{} `json:"params"`
	Name   string        `json:"name"`
	SQL    string        `json:"sql"`
	Steps  []db.Step     `json:"steps"`
}

type transportResponse struct {
	ID     string      `json:"id"`
	OK     bool        `json:"ok"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

func New(cfg config.Config) (*Server, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout())
	defer cancel()

	store, err := db.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}

	s := &Server{cfg: cfg, store: store}
	s.hub = realtime.New(store, cfg.PollInterval(), cfg.QueryTimeout())
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
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.QueryTimeout())
	defer cancel()

	result, dur, err := s.dispatchTransport(ctx, req)
	s.logQuery(req.Op, req.Query, dur, err)
	if err != nil {
		return transportResponse{ID: req.ID, OK: false, Error: err.Error()}
	}
	return transportResponse{ID: req.ID, OK: true, Result: result}
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
	case "prepare":
		start := time.Now()
		return map[string]interface{}{"ok": true, "name": req.Name}, time.Since(start), s.store.Prepare(req.Name, req.SQL)
	case "transaction":
		return s.store.Transaction(ctx, req.Steps)
	default:
		return nil, 0, fmt.Errorf("unsupported transport op %q", req.Op)
	}
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
