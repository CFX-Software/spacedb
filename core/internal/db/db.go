package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/inkwell/spacedb/core/internal/config"
)

type Store struct {
	sqlDB   *sql.DB
	cfg     config.Config
	mu      sync.RWMutex
	queries map[string]string
}

type ExecResult struct {
	RowsAffected int64 `json:"rowsAffected"`
	LastInsertID int64 `json:"lastInsertId,omitempty"`
}

type Step struct {
	Query  string        `json:"query"`
	Params []interface{} `json:"params"`
	Mode   string        `json:"mode"`
}

type StepResult struct {
	Rows         []map[string]interface{} `json:"rows,omitempty"`
	RowsAffected int64                    `json:"rowsAffected,omitempty"`
	LastInsertID int64                    `json:"lastInsertId,omitempty"`
}

func Open(ctx context.Context, cfg config.Config) (*Store, error) {
	driver := cfg.Database.Driver
	if driver == "postgres" {
		driver = "pgx"
	}
	if driver == "mariadb" {
		driver = "mysql"
	}

	sqlDB, err := sql.Open(driver, cfg.Database.DSN)
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(time.Duration(cfg.Database.ConnMaxLifetimeSeconds) * time.Second)

	pingCtx, cancel := context.WithTimeout(ctx, cfg.QueryTimeout())
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	return &Store{sqlDB: sqlDB, cfg: cfg, queries: map[string]string{}}, nil
}

func (s *Store) Close() error {
	return s.sqlDB.Close()
}

func (s *Store) Prepare(name, sqlText string) error {
	if name == "" {
		return errors.New("prepare name is required")
	}
	if sqlText == "" {
		return errors.New("prepare sql is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries[name] = sqlText
	return nil
}

func (s *Store) resolve(query string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if prepared, ok := s.queries[query]; ok {
		return s.rebind(prepared)
	}
	return s.rebind(query)
}

func (s *Store) Query(ctx context.Context, query string, params []interface{}) ([]map[string]interface{}, time.Duration, error) {
	start := time.Now()
	rows, err := s.sqlDB.QueryContext(ctx, s.resolve(query), params...)
	if err != nil {
		return nil, time.Since(start), err
	}
	defer rows.Close()

	result, err := scanRows(rows)
	return result, time.Since(start), err
}

func (s *Store) Single(ctx context.Context, query string, params []interface{}) (map[string]interface{}, time.Duration, error) {
	rows, dur, err := s.Query(ctx, query, params)
	if err != nil || len(rows) == 0 {
		return nil, dur, err
	}
	return rows[0], dur, nil
}

func (s *Store) Execute(ctx context.Context, query string, params []interface{}) (ExecResult, time.Duration, error) {
	start := time.Now()
	result, err := s.sqlDB.ExecContext(ctx, s.resolve(query), params...)
	dur := time.Since(start)
	if err != nil {
		return ExecResult{}, dur, err
	}

	rowsAffected, _ := result.RowsAffected()
	lastInsertID, _ := result.LastInsertId()
	return ExecResult{RowsAffected: rowsAffected, LastInsertID: lastInsertID}, dur, nil
}

func (s *Store) Transaction(ctx context.Context, steps []Step) ([]StepResult, time.Duration, error) {
	start := time.Now()
	tx, err := s.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, time.Since(start), err
	}

	results := make([]StepResult, 0, len(steps))
	for _, step := range steps {
		sqlText := s.resolve(step.Query)
		switch step.Mode {
		case "", "execute":
			res, err := tx.ExecContext(ctx, sqlText, step.Params...)
			if err != nil {
				_ = tx.Rollback()
				return nil, time.Since(start), err
			}
			affected, _ := res.RowsAffected()
			lastID, _ := res.LastInsertId()
			results = append(results, StepResult{RowsAffected: affected, LastInsertID: lastID})
		case "query", "single":
			rows, err := tx.QueryContext(ctx, sqlText, step.Params...)
			if err != nil {
				_ = tx.Rollback()
				return nil, time.Since(start), err
			}
			scanned, err := scanRows(rows)
			_ = rows.Close()
			if err != nil {
				_ = tx.Rollback()
				return nil, time.Since(start), err
			}
			if step.Mode == "single" && len(scanned) > 1 {
				scanned = scanned[:1]
			}
			results = append(results, StepResult{Rows: scanned})
		default:
			_ = tx.Rollback()
			return nil, time.Since(start), fmt.Errorf("unsupported transaction step mode %q", step.Mode)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, time.Since(start), err
	}
	return results, time.Since(start), nil
}

func (s *Store) Stats() map[string]interface{} {
	stats := s.sqlDB.Stats()
	s.mu.RLock()
	prepared := len(s.queries)
	s.mu.RUnlock()
	return map[string]interface{}{
		"maxOpenConnections": stats.MaxOpenConnections,
		"openConnections":    stats.OpenConnections,
		"inUse":              stats.InUse,
		"idle":               stats.Idle,
		"waitCount":          stats.WaitCount,
		"waitDurationMs":     stats.WaitDuration.Milliseconds(),
		"maxIdleClosed":      stats.MaxIdleClosed,
		"maxIdleTimeClosed":  stats.MaxIdleTimeClosed,
		"maxLifetimeClosed":  stats.MaxLifetimeClosed,
		"preparedQueries":    prepared,
	}
}

func (s *Store) rebind(query string) string {
	if s.cfg.Database.Driver != "postgres" || !strings.Contains(query, "?") {
		return query
	}

	var b strings.Builder
	index := 1
	inSingle := false
	inDouble := false
	escaped := false

	for _, r := range query {
		switch {
		case escaped:
			b.WriteRune(r)
			escaped = false
		case r == '\\':
			b.WriteRune(r)
			escaped = true
		case r == '\'' && !inDouble:
			inSingle = !inSingle
			b.WriteRune(r)
		case r == '"' && !inSingle:
			inDouble = !inDouble
			b.WriteRune(r)
		case r == '?' && !inSingle && !inDouble:
			b.WriteString(fmt.Sprintf("$%d", index))
			index++
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := []map[string]interface{}{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := map[string]interface{}{}
		for i, col := range columns {
			switch v := values[i].(type) {
			case []byte:
				row[col] = string(v)
			default:
				row[col] = v
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
