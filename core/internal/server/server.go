package server

import (
	"context"
	"errors"
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
	cfg     config.Config
	store   *db.Store
	hub     *realtime.Hub
	cache   *cache.Cache
	http    *http.Server
	tcp     net.Listener
	metrics *metricsByOp

	subsMu sync.RWMutex
	subs   map[*transportSub]struct{}
}

func New(cfg config.Config) (*Server, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout())
	defer cancel()

	store, err := db.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}

	s := &Server{
		cfg:     cfg,
		store:   store,
		subs:    make(map[*transportSub]struct{}),
		metrics: newMetricsByOp(),
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
		slog.Info("spacedb transport listening", "addr", s.cfg.Transport.Listen)
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

func (s *Server) transportConnCount() int {
	s.subsMu.RLock()
	defer s.subsMu.RUnlock()
	return len(s.subs)
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
