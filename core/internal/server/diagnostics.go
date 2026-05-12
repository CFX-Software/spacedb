package server

import (
	"regexp"
	"sync"
	"time"
)

// Version stamped into the diagnostics bundle. Bumped per release tag.
const Version = "0.2.0"

// errorLog keeps a ring buffer of the most recent SQL errors so users
// can grab a bundle for bug reports without us having to chase logs.
type errorLog struct {
	mu      sync.Mutex
	entries []errorEntry
	cap     int
	pos     int
}

type errorEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	Op         string    `json:"op"`
	Query      string    `json:"query,omitempty"`
	Error      string    `json:"error"`
	DurationMs int64     `json:"durationMs"`
}

func newErrorLog(cap int) *errorLog {
	return &errorLog{cap: cap, entries: make([]errorEntry, 0, cap)}
}

func (e *errorLog) record(op, query, errMsg string, dur time.Duration) {
	entry := errorEntry{
		Timestamp:  time.Now().UTC(),
		Op:         op,
		Query:      query,
		Error:      errMsg,
		DurationMs: dur.Milliseconds(),
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.entries) < e.cap {
		e.entries = append(e.entries, entry)
		return
	}
	e.entries[e.pos] = entry
	e.pos = (e.pos + 1) % e.cap
}

func (e *errorLog) snapshot() []errorEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.entries) < e.cap {
		out := make([]errorEntry, len(e.entries))
		copy(out, e.entries)
		return out
	}
	out := make([]errorEntry, 0, e.cap)
	out = append(out, e.entries[e.pos:]...)
	out = append(out, e.entries[:e.pos]...)
	return out
}

// redactDSN masks the password portion of a database DSN so bug reports
// don't leak credentials. Handles both Go MySQL driver style
// (user:pass@tcp(host:port)/db) and Postgres URL style.
var dsnPasswordRe = regexp.MustCompile(`(://[^:/@\s]+:)[^@/\s]+(@)`)
var mysqlPasswordRe = regexp.MustCompile(`^([^:@\s]+:)[^@\s]+(@tcp)`)

func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	if mysqlPasswordRe.MatchString(dsn) {
		return mysqlPasswordRe.ReplaceAllString(dsn, "${1}****${2}")
	}
	return dsnPasswordRe.ReplaceAllString(dsn, "${1}****${2}")
}
