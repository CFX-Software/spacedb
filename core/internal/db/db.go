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
	stmtMu  sync.RWMutex
	stmts   map[string]*sql.Stmt
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

	return &Store{sqlDB: sqlDB, cfg: cfg, queries: map[string]string{}, stmts: map[string]*sql.Stmt{}}, nil
}

func (s *Store) Close() error {
	s.stmtMu.Lock()
	for _, stmt := range s.stmts {
		_ = stmt.Close()
	}
	s.stmtMu.Unlock()
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

func (s *Store) resolveRaw(query string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if prepared, ok := s.queries[query]; ok {
		return prepared
	}
	return query
}

func (s *Store) resolve(query string) string {
	return s.rebind(s.resolveRaw(query))
}

func (s *Store) Query(ctx context.Context, query string, params []interface{}) ([]map[string]interface{}, time.Duration, error) {
	start := time.Now()
	stmt, err := s.statement(ctx, query)
	if err != nil {
		return nil, time.Since(start), err
	}
	rows, err := stmt.QueryContext(ctx, params...)
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
	stmt, err := s.statement(ctx, query)
	if err != nil {
		return ExecResult{}, time.Since(start), err
	}
	result, err := stmt.ExecContext(ctx, params...)
	dur := time.Since(start)
	if err != nil {
		return ExecResult{}, dur, err
	}

	rowsAffected, _ := result.RowsAffected()
	lastInsertID, _ := result.LastInsertId()
	return ExecResult{RowsAffected: rowsAffected, LastInsertID: lastInsertID}, dur, nil
}

func (s *Store) ExecuteMany(ctx context.Context, query string, rows [][]interface{}) (ExecResult, time.Duration, error) {
	start := time.Now()
	if len(rows) == 0 {
		return ExecResult{}, time.Since(start), nil
	}

	rawSQL := s.resolveRaw(query)
	builder, placeholders, ok := splitInsertValues(rawSQL)
	if !ok {
		return s.executeManyTransaction(ctx, query, rows, start)
	}

	total := ExecResult{}
	for i := 0; i < len(rows); i += 500 {
		end := i + 500
		if end > len(rows) {
			end = len(rows)
		}

		sqlText, params, err := builder(end - i)
		if err != nil {
			return ExecResult{}, time.Since(start), err
		}
		params = params[:0]
		for _, row := range rows[i:end] {
			if len(row) != placeholders {
				return ExecResult{}, time.Since(start), fmt.Errorf("executeMany row has %d params, expected %d", len(row), placeholders)
			}
			params = append(params, row...)
		}

		result, err := s.sqlDB.ExecContext(ctx, s.rebind(sqlText), params...)
		if err != nil {
			return ExecResult{}, time.Since(start), err
		}
		affected, _ := result.RowsAffected()
		lastID, _ := result.LastInsertId()
		total.RowsAffected += affected
		if total.LastInsertID == 0 {
			total.LastInsertID = lastID
		}
	}

	return total, time.Since(start), nil
}

func (s *Store) executeManyTransaction(ctx context.Context, query string, rows [][]interface{}, start time.Time) (ExecResult, time.Duration, error) {
	steps := make([]Step, len(rows))
	for i, params := range rows {
		steps[i] = Step{Query: query, Params: params, Mode: "execute"}
	}
	results, dur, err := s.Transaction(ctx, steps)
	if err != nil {
		return ExecResult{}, dur, err
	}
	total := ExecResult{}
	for _, result := range results {
		total.RowsAffected += result.RowsAffected
		if total.LastInsertID == 0 {
			total.LastInsertID = result.LastInsertID
		}
	}
	return total, time.Since(start), nil
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

func splitInsertValues(sqlText string) (func(int) (string, []interface{}, error), int, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sqlText, ";"))
	lower := strings.ToLower(trimmed)
	index := strings.LastIndex(lower, " values ")
	if !strings.HasPrefix(strings.TrimSpace(lower), "insert ") || index < 0 {
		return nil, 0, false
	}

	prefix := trimmed[:index+len(" values ")]
	group := strings.TrimSpace(trimmed[index+len(" values "):])
	if !strings.HasPrefix(group, "(") || !strings.HasSuffix(group, ")") {
		return nil, 0, false
	}

	placeholders := strings.Count(group, "?")
	if placeholders == 0 {
		return nil, 0, false
	}

	return func(count int) (string, []interface{}, error) {
		if count <= 0 {
			return "", nil, errors.New("executeMany batch count must be positive")
		}
		groups := make([]string, count)
		for i := range groups {
			groups[i] = group
		}
		return prefix + strings.Join(groups, ","), make([]interface{}, 0, count*placeholders), nil
	}, placeholders, true
}

func (s *Store) statement(ctx context.Context, query string) (*sql.Stmt, error) {
	sqlText := s.resolve(query)

	s.stmtMu.RLock()
	stmt, ok := s.stmts[sqlText]
	s.stmtMu.RUnlock()
	if ok {
		return stmt, nil
	}

	prepared, err := s.sqlDB.PrepareContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}

	s.stmtMu.Lock()
	defer s.stmtMu.Unlock()
	if existing, ok := s.stmts[sqlText]; ok {
		_ = prepared.Close()
		return existing, nil
	}
	s.stmts[sqlText] = prepared
	return prepared, nil
}

func (s *Store) Stats() map[string]interface{} {
	stats := s.sqlDB.Stats()
	s.mu.RLock()
	prepared := len(s.queries)
	s.mu.RUnlock()
	s.stmtMu.RLock()
	cachedStatements := len(s.stmts)
	s.stmtMu.RUnlock()
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
		"cachedStatements":   cachedStatements,
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
