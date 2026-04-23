package server

import (
	"context"
	"errors"
	"fmt"
	"time"
)

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

// cacheGetMany batches N getById lookups into a single transport call.
// Returns `{rows: { "<key>": row, ... }, missing: [keys not found in DB]}`.
// Amortizes the FiveM export overhead across many rows; on a cache-warm
// pass every key hits in-process Go memory and the SQL fetch is skipped.
func (s *Server) cacheGetMany(ctx context.Context, req transportRequest) (interface{}, time.Duration, error) {
	if req.Table == "" {
		return nil, 0, errors.New("cacheGetMany requires table")
	}
	if len(req.Keys) == 0 {
		return map[string]interface{}{"rows": map[string]interface{}{}, "missing": []interface{}{}}, 0, nil
	}
	pkColumn := req.PKColumn
	if pkColumn == "" {
		pkColumn = "id"
	}
	if !validIdent.MatchString(req.Table) || !validIdent.MatchString(pkColumn) {
		return nil, 0, fmt.Errorf("invalid identifier table=%q pkColumn=%q", req.Table, pkColumn)
	}

	rows := make(map[string]interface{}, len(req.Keys))
	misses := make([]interface{}, 0, len(req.Keys))
	missStrings := make([]string, 0, len(req.Keys))
	for _, raw := range req.Keys {
		k := keyToString(raw)
		if row, ok := s.cache.Get(req.Table, k); ok {
			rows[k] = row
			continue
		}
		misses = append(misses, raw)
		missStrings = append(missStrings, k)
	}

	var totalDur time.Duration
	if len(misses) > 0 {
		placeholders := make([]byte, 0, len(misses)*2)
		for i := range misses {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
		}
		sql := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)", req.Table, pkColumn, placeholders)
		fetched, dur, err := s.store.Query(ctx, sql, misses)
		totalDur = dur
		if err != nil {
			return nil, dur, err
		}
		// fetched is []map[string]interface{}; route each back by pk value.
		stillMissing := make([]interface{}, 0)
		seen := make(map[string]bool, len(missStrings))
		for _, row := range fetched {
			if row == nil {
				continue
			}
			pkVal := row[pkColumn]
			k := keyToString(pkVal)
			s.cache.Set(req.Table, k, row)
			rows[k] = row
			seen[k] = true
		}
		// Track which requested keys had no row in the DB so caller knows.
		for i, k := range missStrings {
			if !seen[k] {
				stillMissing = append(stillMissing, misses[i])
			}
		}
		return map[string]interface{}{"rows": rows, "missing": stillMissing}, totalDur, nil
	}

	return map[string]interface{}{"rows": rows, "missing": misses}, 0, nil
}

// keyToString stringifies a JSON-unmarshalled value the same way the cache
// expects. Floats that are integers come back without a decimal.
func keyToString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%v", x)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
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
